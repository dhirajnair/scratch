#!/usr/bin/env bash
set -euo pipefail

# Enable debug mode if DEBUG=1
[ "${DEBUG:-0}" = "1" ] && set -x

# Configuration
ACCOUNT="dataingestiondl"
CONTAINER="raw-data"
PREFIX="1.Corporate_Registry/BVD/Regions/ExcelExtraction/China/Financials/30-April-2025/1-4000/"

# Generate timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT="blob_sizes_${TIMESTAMP}.csv"
LOG_FILE="blob_sizes_${TIMESTAMP}.log"
TEMP_DATA=$(mktemp)

# Cleanup on exit
trap "rm -f $TEMP_DATA" EXIT

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Function to log errors
log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

# Start logging
log "========================================="
log "Starting Azure Blob Analysis Script"
log "========================================="
log "Configuration:"
log "  Account: $ACCOUNT"
log "  Container: $CONTAINER"
log "  Prefix: $PREFIX"
log "  Output file: $OUTPUT"
log "  Log file: $LOG_FILE"
log "  Temp file: $TEMP_DATA"

# Tools check
if ! command -v az >/dev/null 2>&1; then
    log_error "'az' CLI is not installed. Install Azure CLI and retry."
    exit 1
fi
log "Azure CLI found: $(which az)"

# Check Azure CLI version
log "Azure CLI version:"
az version 2>&1 | tee -a "$LOG_FILE"

# Test authentication
log "Testing Azure authentication..."
if ! az account show >/dev/null 2>&1; then
    log_error "Not logged in to Azure. Run 'az login' first."
    exit 1
fi
log "Azure authentication successful"

# Write temporary data header
echo "filename,size_mb" > "$TEMP_DATA"
log "Created temp file with header"

# Build the Azure CLI command
log "Building Azure CLI command..."
AZ_CMD="az storage blob list --account-name $ACCOUNT --container-name $CONTAINER --prefix \"$PREFIX\" --output json"
log "Command: $AZ_CMD"

# Method 1: Try with JSON output for better debugging
log "Attempting to fetch blob list (JSON format for debugging)..."
BLOB_JSON=$(mktemp)
trap "rm -f $TEMP_DATA $BLOB_JSON" EXIT

if az storage blob list \
    --account-name "$ACCOUNT" \
    --container-name "$CONTAINER" \
    --prefix "$PREFIX" \
    --output json > "$BLOB_JSON" 2>> "$LOG_FILE"; then

    log "Azure command succeeded. Checking JSON output..."

    # Check if we got any data
    json_size=$(stat -f%z "$BLOB_JSON" 2>/dev/null || stat -c%s "$BLOB_JSON" 2>/dev/null || echo "0")
    log "JSON file size: $json_size bytes"

    if [ "$json_size" -gt 2 ]; then
        # Count blobs in JSON
        blob_count=$(cat "$BLOB_JSON" | grep -c '"name"' || echo "0")
        log "Found $blob_count blobs in JSON response"

        # Process JSON with Python if available
        if command -v python3 >/dev/null 2>&1; then
            log "Processing with Python..."
            python3 -c "
import json
import sys

try:
    with open('$BLOB_JSON', 'r') as f:
        blobs = json.load(f)

    print(f'Processing {len(blobs)} blobs...')

    with open('$TEMP_DATA', 'w') as out:
        out.write('filename,size_mb\\n')
        for blob in blobs:
            name = blob.get('name', '')
            size = blob.get('properties', {}).get('contentLength', 0)
            size_mb = size / 1048576
            out.write(f'\"{name}\",{size_mb:.2f}\\n')

    print(f'Wrote {len(blobs)} entries to temp file')

except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1 | tee -a "$LOG_FILE"

        elif command -v jq >/dev/null 2>&1; then
            log "Processing with jq..."
            cat "$BLOB_JSON" | jq -r '.[] | [.name, (.properties.contentLength / 1048576)] | @csv' >> "$TEMP_DATA" 2>> "$LOG_FILE"

        else
            log "Neither Python nor jq available. Trying awk/grep fallback..."
            # Fallback: try to parse JSON with basic tools
            grep -o '"name":"[^"]*"' "$BLOB_JSON" | cut -d'"' -f4 > /tmp/names.txt
            grep -o '"contentLength":[0-9]*' "$BLOB_JSON" | cut -d':' -f2 > /tmp/sizes.txt

            if [ -s /tmp/names.txt ] && [ -s /tmp/sizes.txt ]; then
                paste -d',' /tmp/names.txt /tmp/sizes.txt | while IFS=',' read -r name size; do
                    size_mb=$(awk "BEGIN {printf \"%.2f\", $size / 1048576}")
                    echo "\"$name\",$size_mb" >> "$TEMP_DATA"
                done
            fi
            rm -f /tmp/names.txt /tmp/sizes.txt
        fi
    else
        log_error "JSON file is empty or too small"
    fi
else
    log_error "Azure CLI command failed. Check error messages above."
    log "Trying with TSV output as fallback..."

    # Fallback to TSV
    az storage blob list \
        --account-name "$ACCOUNT" \
        --container-name "$CONTAINER" \
        --prefix "$PREFIX" \
        --query "[*].[name, properties.contentLength]" \
        --output tsv 2>> "$LOG_FILE" | while IFS=$'\t' read -r name size; do
        if [ -n "$name" ] && [ -n "$size" ]; then
            size_mb=$(awk "BEGIN {printf \"%.2f\", $size / 1048576}")
            echo "\"$name\",$size_mb" >> "$TEMP_DATA"
            log "Added: $name ($size_mb MB)"
        fi
    done
fi

# Check what we have in temp file
log "Checking temp file contents..."
temp_lines=$(wc -l < "$TEMP_DATA")
log "Temp file has $temp_lines lines (including header)"

if [ "$temp_lines" -le 1 ]; then
    log_error "No data retrieved! Temp file is empty."
    log "Attempting alternative method with explicit auth..."

    # Try with explicit connection string or SAS token
    log "Do you have a SAS token or connection string? If so, set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_SAS_TOKEN"

    # Try simplified command
    log "Trying simplified command..."
    az storage blob list \
        --account-name "$ACCOUNT" \
        --container-name "$CONTAINER" \
        --prefix "$PREFIX" 2>&1 | tee -a "$LOG_FILE" | head -20

    exit 1
fi

# Calculate statistics
log "Calculating statistics..."
count_lt_10=$(tail -n +2 "$TEMP_DATA" | awk -F',' '{gsub(/"/, "", $2); if ($2 != "" && $2 < 10) count++} END {print count+0}')
count_10_50=$(tail -n +2 "$TEMP_DATA" | awk -F',' '{gsub(/"/, "", $2); if ($2 != "" && $2 >= 10 && $2 <= 50) count++} END {print count+0}')
count_gt_50=$(tail -n +2 "$TEMP_DATA" | awk -F',' '{gsub(/"/, "", $2); if ($2 != "" && $2 > 50) count++} END {print count+0}')
total_count=$((temp_lines - 1))
total_size_mb=$(tail -n +2 "$TEMP_DATA" | awk -F',' '{gsub(/"/, "", $2); if ($2 != "") sum+=$2} END {printf "%.2f", sum}')

log "Statistics calculated:"
log "  Total files: $total_count"
log "  Total size: $total_size_mb MB"
log "  Files < 10 MB: $count_lt_10"
log "  Files 10-50 MB: $count_10_50"
log "  Files > 50 MB: $count_gt_50"

# Create final CSV
log "Creating output file: $OUTPUT"
{
    echo "SUMMARY REPORT"
    echo "Generated On,$(date +"%Y-%m-%d %H:%M:%S")"
    echo "Storage Account,$ACCOUNT"
    echo "Container,$CONTAINER"
    echo "Path Analyzed,$PREFIX"
    echo ""
    echo "FILE SIZE DISTRIBUTION"
    echo "Total Files,$total_count"
    echo "Total Size (MB),$total_size_mb"
    echo "Files < 10 MB,$count_lt_10"
    echo "Files 10-50 MB,$count_10_50"
    echo "Files > 50 MB,$count_gt_50"
    echo ""
    echo "DETAILED FILE LIST"
    cat "$TEMP_DATA"
} > "$OUTPUT"

# Verify output
if [ -f "$OUTPUT" ]; then
    output_size=$(wc -l < "$OUTPUT")
    log "SUCCESS: Output file created with $output_size lines"
    log "Output file: $OUTPUT"

    echo ""
    echo "========================================="
    echo "First 20 lines of output:"
    head -n 20 "$OUTPUT"
    echo "========================================="
else
    log_error "Output file was not created!"
    exit 1
fi

log "Script completed successfully"
log "Check $LOG_FILE for detailed execution log"

# Cleanup
rm -f "$BLOB_JSON"