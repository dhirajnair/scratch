#!/usr/bin/env bash
set -euo pipefail

# Configuration
ACCOUNT="dataingestiondl"
CONTAINER="raw-data"
PREFIX="1.Corporate_Registry/BVD/Regions/ExcelExtraction/China/Financials/30-April-2025/4001-8000/"
OUTPUT="dups.txt"

# Tools check
command -v az >/dev/null 2>&1 || { echo "ERROR: 'az' is required. Install Azure CLI and retry."; exit 1; }

echo "Fetching blobs matching criteria from prefix: $PREFIX"

# Direct approach - redirect stderr to avoid contaminating output
az storage blob list \
  --account-name "$ACCOUNT" \
  --container-name "$CONTAINER" \
  --prefix "$PREFIX" \
  --query "[? (contains(name,'(1)') || contains(name,'(2)') || contains(name,'(3)') || contains(name,'(4)') || contains(name,'(5)')) && (ends_with(name,'.xlsx') || ends_with(name,'.xls') || ends_with(name,'.xlsm')) ].[join('', ['raw-data/', name])]" \
  --output tsv 2>/dev/null > "$OUTPUT"

echo "Done. Total matches: $(wc -l < "$OUTPUT")"
echo "Saved to $OUTPUT"

# Optional: If you need pagination due to large result sets, use this alternative:
if false; then  # Change to true if you need pagination
  echo "Using paginated approach..."

  TEMP_FILE=$(mktemp)
  trap "rm -f $TEMP_FILE" EXIT

  > "$OUTPUT"  # Clear output file
  nextMarker=""
  page=0

  while : ; do
    page=$((page+1))
    echo "Fetching page #$page..."

    # Build command with optional marker
    cmd=(az storage blob list
         --account-name "$ACCOUNT"
         --container-name "$CONTAINER"
         --prefix "$PREFIX"
         --num-results 1000)

    if [ -n "${nextMarker:-}" ]; then
      cmd+=(--marker "$nextMarker")
    fi

    # Execute and filter
    "${cmd[@]}" \
      --query "[? (contains(name,'(1)') || contains(name,'(2)') || contains(name,'(3)') || contains(name,'(4)') || contains(name,'(5)')) && (ends_with(name,'.xlsx') || ends_with(name,'.xls') || ends_with(name,'.xlsm')) ].[join('', ['raw-data/', name])]" \
      --output tsv >> "$OUTPUT"

    # Get next marker
    nextMarker=$("${cmd[@]}" --query "nextMarker" --output tsv 2>/dev/null || echo "")

    if [ -z "${nextMarker:-}" ] || [ "$nextMarker" = "null" ]; then
      echo "No more pages. Finished after $page page(s)."
      break
    fi
  done
fi