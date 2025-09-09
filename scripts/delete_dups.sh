#!/usr/bin/env bash
set -euo pipefail

# delete_dups_robust.sh
# Usage:
#   ./delete_dups_robust.sh <mode> [--dry-run] [--limit N]
# Modes: login | key | sas
# Examples:
#   ./delete_dups_robust.sh key           # interactive account-key prompt + confirm
#   ./delete_dups_robust.sh sas --dry-run # show what would be deleted
#   LIMIT example: ./delete_dups_robust.sh key --limit 100

ACCOUNT="dataingestiondl"
CONTAINER="raw-data"
INPUT_FILE="dups.txt"
FAILED_FILE="failed.txt"
DELETED_FILE="dups_deleted.txt"
DRY_RUN=false
LIMIT=0   # 0 = no limit
RETRIES=3

# --- parse args
if [ $# -lt 1 ]; then
  echo "Usage: $0 <mode> [--dry-run] [--limit N]"
  echo " mode: login | key | sas"
  exit 2
fi

MODE="$1"
shift || true

# parse optional flags
while (( $# )); do
  case "$1" in
    --dry-run|-n) DRY_RUN=true; shift ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --help|-h) echo "Usage: $0 <mode> [--dry-run] [--limit N]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 2 ;;
  esac
done

# --- checks
for cmd in az jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: '$cmd' is required. Install it and retry."
    exit 1
  fi
done

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "ERROR: Input file '$INPUT_FILE' not found. One blob path per line."
  exit 1
fi

# --- auth args
AUTH_ARGS=()
case "$MODE" in
  login)
    echo "Using AAD login mode. This requires Storage Blob Data Contributor/Owner."
    AUTH_ARGS+=(--auth-mode login)
    ;;
  key)
    # accept account key from env if set, else prompt (hidden)
    if [[ -z "${ACCOUNT_KEY:-}" ]]; then
      read -rsp "Enter ACCOUNT KEY for $ACCOUNT: " ACCOUNT_KEY
      echo
    fi
    if [[ -z "${ACCOUNT_KEY:-}" ]]; then
      echo "No account key provided; aborting."
      exit 1
    fi
    AUTH_ARGS+=(--account-key "$ACCOUNT_KEY")
    ;;
  sas)
    if [[ -z "${SAS_TOKEN:-}" ]]; then
      read -rp "Enter SAS token (paste with or without leading '?'): " SAS_TOKEN_RAW
      SAS_TOKEN="${SAS_TOKEN_RAW#\?}"
    else
      SAS_TOKEN="${SAS_TOKEN#\?}"
    fi
    if [[ -z "${SAS_TOKEN:-}" ]]; then
      echo "No SAS token provided; aborting."
      exit 1
    fi
    AUTH_ARGS+=(--sas-token "$SAS_TOKEN")
    ;;
  *)
    echo "Unknown mode: $MODE. Use login|key|sas"
    exit 2
    ;;
esac

# --- normalize and confirm
TOTAL_LINES=$(sed -n '/\S/ p' "$INPUT_FILE" | wc -l | tr -d ' ')
echo "Found $TOTAL_LINES non-empty lines in $INPUT_FILE"
if [[ "$LIMIT" -gt 0 ]]; then
  echo "LIMIT is set to $LIMIT (will process up to $LIMIT entries)."
fi

echo "DRY RUN: $DRY_RUN"
read -rp "Proceed with ${MODE^^} deletion? Type 'yes' to continue: " yn_raw
yn="$(printf '%s' "$yn_raw" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g' | tr '[:upper:]' '[:lower:]')"
if [[ "$yn" != "yes" && "$yn" != "y" ]]; then
  echo "Aborted by user."
  exit 0
fi

# --- helpers
normalize_blob_name() {
  local line="$1"
  # trim
  line="$(printf '%s' "$line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  [[ -z "$line" ]] && { printf ''; return; }

  # If a full URL (contains '://'), try to extract after '/<container>/'
  if [[ "$line" =~ :// ]]; then
    # look for /<container>/ in the URL and strip everything up to and including that
    # e.g., https://acct.blob.core.windows.net/raw-data/path/to/blob -> path/to/blob
    blob="${line#*"/$CONTAINER/"}"
    if [[ "$blob" == "$line" ]]; then
      # fallback: use everything after last slash
      blob="${line##*/}"
    fi
    printf '%s' "$blob"
    return
  fi

  # If starts with container/, strip that
  if [[ "$line" == "$CONTAINER/"* ]]; then
    printf '%s' "${line#${CONTAINER}/}"
    return
  fi

  # else assume it's already a blob path
  printf '%s' "$line"
}

delete_blob_once() {
  local blob="$1"
  if az storage blob delete \
      --account-name "$ACCOUNT" \
      --container-name "$CONTAINER" \
      --name "$blob" \
      "${AUTH_ARGS[@]}" \
      --only-show-errors; then
    return 0
  else
    return 1
  fi
}

# --- main loop
: > "$FAILED_FILE"
count=0
processed=0
deleted_count=0

while IFS= read -r rawline || [ -n "${rawline:-}" ]; do
  # skip empty
  [[ -z "${rawline// }" ]] && continue

  count=$((count+1))
  if [[ "$LIMIT" -gt 0 && "$processed" -ge "$LIMIT" ]]; then
    break
  fi

  blob_name="$(normalize_blob_name "$rawline")"
  [[ -z "$blob_name" ]] && continue

  processed=$((processed+1))
  echo "[$processed/$TOTAL_LINES] -> $blob_name"

  if $DRY_RUN; then
    continue
  fi

  # try with retries
  attempt=1
  success=false
  while [ $attempt -le $RETRIES ]; do
    if delete_blob_once "$blob_name"; then
      echo "  ✅ deleted"
      # Log to deleted file
      echo "$rawline" >> "$DELETED_FILE"
      deleted_count=$((deleted_count+1))
      success=true
      break
    else
      echo "  ⚠️  delete failed (attempt $attempt/$RETRIES) — retrying..."
      attempt=$((attempt+1))
      sleep 1
    fi
  done

  if ! $success; then
    echo "$blob_name" >> "$FAILED_FILE"
    echo "  ❌ logged failed delete to $FAILED_FILE"
  fi
done < "$INPUT_FILE"

# --- cleanup and finalize
if ! $DRY_RUN && [[ "$deleted_count" -gt 0 ]]; then
  echo "Appended $deleted_count successfully deleted entries to $DELETED_FILE"

  # Create backup of original dups.txt before deleting
  if cp "$INPUT_FILE" "${INPUT_FILE}.backup.$(date +%Y%m%d_%H%M%S)"; then
    echo "Created backup: ${INPUT_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
  fi

  # Delete the original dups.txt file
  if rm "$INPUT_FILE"; then
    echo "Deleted original $INPUT_FILE file"
  else
    echo "⚠️  Failed to delete $INPUT_FILE - you may want to remove it manually"
  fi
fi

# --- cleanup sensitive envs if any
if [[ "${ACCOUNT_KEY:-}" != "" ]]; then
  unset ACCOUNT_KEY || true
fi
if [[ "${SAS_TOKEN:-}" != "" ]]; then
  unset SAS_TOKEN || true
fi

echo "Done. Processed $processed entries."
if [[ -s "$FAILED_FILE" ]]; then
  echo "Some deletions failed — see $FAILED_FILE ( $(wc -l < "$FAILED_FILE") lines )"
else
  echo "All deletions succeeded (no failures logged)."
fi

if ! $DRY_RUN; then
  echo "Summary:"
  echo "  - Successfully deleted: $deleted_count blobs"
  echo "  - Failed deletions: $(wc -l < "$FAILED_FILE" 2>/dev/null || echo 0) blobs"
  echo "  - Deleted entries logged to: $DELETED_FILE"
  if [[ "$deleted_count" -gt 0 ]]; then
    echo "  - Original $INPUT_FILE has been removed (backup created)"
  fi
fi