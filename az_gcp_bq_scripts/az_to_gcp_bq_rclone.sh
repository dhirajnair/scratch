#!/usr/bin/env bash
#
# process_folders_rclone_bq.sh
#
# Process comma-separated PROCESS_FOLDERS: sync parquet from Azure → GCS, then load into BQ.
# Features:
#   - RCLONE_MODE=sync|copy
#   - SKIP_BQ=true to skip BQ loads
#   - --dry-run to list planned actions only
#   - Auto-loads .env in the script directory
#
set -o pipefail
trap 'echo "$(date --iso-8601=seconds) [ERROR] Interrupted"; exit 2' INT TERM

# --- auto-load .env ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loading environment from $SCRIPT_DIR/.env"
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/.env"
fi

# --- CLI flags ---------------------------------------------------------------
DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1
  shift
fi

# ---------------- CONFIG -----------------------------------------------------
PROCESS_FOLDERS="${PROCESS_FOLDERS:-}"      # REQUIRED
AZ_CONTAINER="${AZ_CONTAINER:-merged-parquet}"
AZ_REMOTE="${AZ_REMOTE:-azure}"             # rclone remote for Azure
GCS_REMOTE="${GCS_REMOTE:-gcs}"             # rclone remote for GCS
GCP_BUCKET="${GCP_BUCKET:-}"                # REQUIRED

BQ_PROJECT="${BQ_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-cq_masters}"
BQ_LOCATION="${BQ_LOCATION:-US}"
BQ_WRITE_DISPOSITION="${BQ_WRITE_DISPOSITION:-WRITE_TRUNCATE}"

RCLONE_MODE="${RCLONE_MODE:-sync}"          # sync (mirror) or copy (no deletes)
BQ_CONCURRENCY="${BQ_CONCURRENCY:-4}"
MAX_RETRIES="${MAX_RETRIES:-3}"
SLEEP_BETWEEN_RETRY="${SLEEP_BETWEEN_RETRY:-4}"

SKIP_BQ="${SKIP_BQ:-false}"

WORK_DIR="${WORK_DIR:-/tmp/process_folders_rclone}"
mkdir -p "$WORK_DIR"
# ---------------------------------------------------------------------------

# ---------------- logging helpers ------------------------------------------
log()  { printf '%s [%s] %s\n' "$(date --iso-8601=seconds)" "$1" "$2"; }
info() { log INFO "$*"; }
warn() { log WARN "$*"; }
die()  { log ERROR "$*"; exit 1; }
# ---------------------------------------------------------------------------

# ---------------- validation -----------------------------------------------
[ -n "$PROCESS_FOLDERS" ] || die "PROCESS_FOLDERS is required (comma-separated)"
[ -n "$GCP_BUCKET" ] || die "GCP_BUCKET is required"

command -v rclone >/dev/null 2>&1 || die "rclone required"
if [ "$SKIP_BQ" != "true" ]; then
  command -v bq >/dev/null 2>&1 || die "bq required"
fi

IFS=',' read -r -a FOLDERS_ARR <<< "$PROCESS_FOLDERS"
CLEAN_FOLDERS=()
for f in "${FOLDERS_ARR[@]}"; do
  f="$(echo "$f" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')" # trim
  f="${f#/}"; f="${f%/}"
  [ -n "$f" ] && CLEAN_FOLDERS+=("$f")
done
[ "${#CLEAN_FOLDERS[@]}" -gt 0 ] || die "No valid folders found in PROCESS_FOLDERS"

info "Folders to process: ${CLEAN_FOLDERS[*]}"
info "Using rclone mode: $RCLONE_MODE"
info "Dry-run: $DRY_RUN"
info "SKIP_BQ: $SKIP_BQ"
# ---------------------------------------------------------------------------

# ---------------- table naming helper --------------------------------------
extract_table_name() {
  local path="$1"
  path="${path#/}"; path="${path%/}"
  IFS='/' read -r -a comps <<< "$path"
  local n=${#comps[@]}
  local name=""
  if [ "$n" -ge 3 ] && [[ "${comps[$((n-1))]}" =~ ^[0-9]{8}$ ]]; then
    idx1=$((n-3)); idx2=$((n-2))
    if [ "$idx1" -lt 0 ]; then name="${comps[$idx2]}"; else name="${comps[$idx1]}_${comps[$idx2]}"; fi
  elif [ "$n" -ge 2 ]; then
    name="${comps[0]}_${comps[1]}"
  elif [ "$n" -eq 1 ]; then
    name="${comps[0]}"
  else
    name="root"
  fi
  name="$(echo "$name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')"
  printf '%s' "$name"
}
export -f extract_table_name
# ---------------------------------------------------------------------------

# ---------------- sync each prefix -----------------------------------------
ALL_PREFIXES=()
for prefix in "${CLEAN_FOLDERS[@]}"; do
  src="${AZ_REMOTE}:${AZ_CONTAINER}/$prefix"
  dst="${GCS_REMOTE}:${GCP_BUCKET}/${GCP_PREFIX}/$prefix"
  info "Syncing $src -> $dst"

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "DRY-RUN: rclone $RCLONE_MODE $src $dst --progress --include '*.parquet' --dry-run"
  else
    if ! rclone $RCLONE_MODE "$src" "$dst" --progress --include "*.parquet"; then
      warn "rclone $RCLONE_MODE failed for $prefix"
      continue
    fi
  fi
  ALL_PREFIXES+=("$prefix")
done

[ "${#ALL_PREFIXES[@]}" -gt 0 ] || { info "No prefixes successfully synced. Exiting."; exit 0; }
# ---------------------------------------------------------------------------

# ---------------- skip BQ if requested -------------------------------------
if [ "$SKIP_BQ" = "true" ]; then
  info "SKIP_BQ=true -> skipping BigQuery loads."
  exit 0
fi

# ---------------- prepare BQ tasks (date-aware) ----------------------------
BQ_TASKS_FILE="$WORK_DIR/bq_tasks.txt"
: > "$BQ_TASKS_FILE"

for prefix in "${ALL_PREFIXES[@]}"; do
  gcs_prefix="gs://$GCP_BUCKET/$prefix"
  mapfile -t FILES < <(gsutil ls -r "$gcs_prefix/**.parquet" 2>/dev/null || true)
  [ "${#FILES[@]}" -eq 0 ] && { warn "No parquet files found in $gcs_prefix, skipping BQ"; continue; }

  declare -A TBL_SEEN=()
  for file in "${FILES[@]}"; do
    rel="${file#gs://$GCP_BUCKET/}"
    tbl="$(extract_table_name "$rel")"
    if [ -z "${TBL_SEEN[$tbl]+x}" ]; then
      dirpath="$(dirname "$rel")"
      IFS='/' read -r -a comps <<< "$dirpath"
      last_comp="${comps[${#comps[@]}-1]}"
      if [[ "$last_comp" =~ ^[0-9]{8}$ ]]; then
        parent_prefix="${dirpath%/*}"
        gcs_pattern="gs://$GCP_BUCKET/$parent_prefix/*/*.parquet"
      else
        gcs_pattern="gs://$GCP_BUCKET/$dirpath/*.parquet"
      fi
      printf '%s\t%s\n' "$tbl" "$gcs_pattern" >> "$BQ_TASKS_FILE"
      TBL_SEEN[$tbl]=1
    fi
  done
done

info "Prepared BigQuery tasks: $(wc -l < "$BQ_TASKS_FILE" 2>/dev/null || echo 0)"
[ -s "$BQ_TASKS_FILE" ] || { info "No BQ tasks; done."; exit 0; }
# ---------------------------------------------------------------------------

# ---------------- run BigQuery loads ---------------------------------------
if [ "$DRY_RUN" -eq 1 ]; then
  echo "=== DRY-RUN BigQuery loads ==="
  while IFS=$'\t' read -r tbl gcs_pattern; do
    echo "bq load --source_format=PARQUET --autodetect ${BQ_PROJECT}:${BQ_DATASET}.${tbl} ${gcs_pattern}"
  done < "$BQ_TASKS_FILE"
  exit 0
fi

bq_load_worker() {
  local tbl="$1"; local gcs_pattern="$2"
  local bq_table="$BQ_PROJECT:$BQ_DATASET.$tbl"
  local attempt=1; local ok=1

  case "$BQ_WRITE_DISPOSITION" in
    WRITE_TRUNCATE) write_flag="--replace" ;;
    WRITE_APPEND)   write_flag="--append_table" ;;
    WRITE_EMPTY)    write_flag="--noreplace" ;;
    *) write_flag="--replace" ;;
  esac

  while [ "$attempt" -le "$MAX_RETRIES" ]; do
    info "BQ load attempt $attempt/$MAX_RETRIES for $bq_table from $gcs_pattern"
    if bq --quiet --project_id="$BQ_PROJECT" load ${write_flag} --source_format=PARQUET --autodetect "$bq_table" "$gcs_pattern"; then
      ok=0; break
    else
      warn "BQ load failed for $bq_table (attempt $attempt)"
      sleep "$SLEEP_BETWEEN_RETRY"
    fi
    attempt=$((attempt+1))
  done

  [ $ok -eq 0 ] || echo "$tbl" >> "$WORK_DIR/bq_failures.txt"
  return $ok
}
export -f bq_load_worker info warn error

rm -f "$WORK_DIR/bq_failures.txt"
if command -v parallel >/dev/null 2>&1; then
  parallel --colsep '\t' -j "$BQ_CONCURRENCY" 'bq_load_worker {1} {2}' :::: "$BQ_TASKS_FILE"
else
  cat "$BQ_TASKS_FILE" | xargs -L1 -P "$BQ_CONCURRENCY" -I '{}' bash -c 't=$(echo "{}" | cut -f1); g=$(echo "{}" | cut -f2); bq_load_worker "$t" "$g"'
fi

bq_failures=0
[ -f "$WORK_DIR/bq_failures.txt" ] && bq_failures=$(wc -l < "$WORK_DIR/bq_failures.txt")
info "BigQuery loads complete. failures=$bq_failures"

exit 0
