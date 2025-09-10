#!/usr/bin/env bash
#
# extract_parquet_schema.sh - Extract and fix BigQuery schema from Parquet files
#
# Usage:
#   ./extract_parquet_schema.sh                    # Process all paths in INPUT_FILE
#   ./extract_parquet_schema.sh <gs://path/...>    # Process single path
#
# This script uses the same .env configuration as load_gcp_paths_to_bq.sh
#
set -euo pipefail

# --- Auto-load .env if exists -----------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loading environment from $SCRIPT_DIR/.env"
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/.env"
fi

# --- Configuration (same as bq loader) --------------------------------------
INPUT_FILE="${INPUT_FILE:-bq_source.txt}"
BQ_PROJECT="${BQ_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-cq_source}"
BQ_LOCATION="${BQ_LOCATION:-US}"

# Schema-specific configuration
SCHEMA_DIR="${SCHEMA_DIR:-schemas}"  # Directory to save schema files
TEMP_DATASET="${TEMP_DATASET:-temp_schema_extraction}"
AUTO_FIX_TYPES="${AUTO_FIX_TYPES:-true}"  # Automatically fix STRING types based on column names
SAMPLE_ROWS="${SAMPLE_ROWS:-1000}"  # Number of rows to sample for schema inference

# --- Parse arguments ---------------------------------------------------------
SINGLE_PATH="${1:-}"

# --- Functions ---------------------------------------------------------------
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

# Infer table name from path (same logic as main loader)
infer_table_name() {
  local gcp_path="$1"

  # Remove gs:// prefix and bucket name
  local path_only="${gcp_path#gs://}"
  path_only="${path_only#*/}"  # Remove bucket name

  # Remove only the filename pattern at the end
  if [[ "$path_only" =~ ^(.*)/\*\.[^/]+$ ]]; then
    path_only="${BASH_REMATCH[1]}"
  elif [[ "$path_only" =~ ^(.*)/[^/]+\.[^/]+$ ]]; then
    path_only="${BASH_REMATCH[1]}"
  fi

  # Split path by / and look for date pattern
  IFS='/' read -ra PATH_PARTS <<< "$path_only"

  local table_name=""
  for i in "${!PATH_PARTS[@]}"; do
    if [[ "${PATH_PARTS[$i]}" =~ ^[0-9]{8}$ ]]; then
      # Found date, use the component before it
      if [ $i -gt 0 ]; then
        table_name="${PATH_PARTS[$((i-1))]}"
        break
      fi
    fi
  done

  # If no date pattern found, use the last component
  if [ -z "$table_name" ]; then
    local num_parts=${#PATH_PARTS[@]}
    if [ $num_parts -gt 0 ]; then
      table_name="${PATH_PARTS[$((num_parts-1))]}"
    fi
  fi

  # Clean up for BQ table naming
  table_name=$(echo "$table_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')

  # Fallback if still empty
  if [ -z "$table_name" ]; then
    table_name="unknown_table_$(date +%s)"
  fi

  echo "$table_name"
}

# Get first parquet file from a wildcard path
get_first_parquet() {
  local gcp_path="$1"

  # If path contains wildcard, get the first file
  if [[ "$gcp_path" =~ \* ]]; then
    # List files and get the first one
    local first_file=$(gsutil ls "$gcp_path" 2>/dev/null | head -n 1)
    if [ -n "$first_file" ]; then
      echo "$first_file"
    else
      echo "$gcp_path"  # Return original if no files found
    fi
  else
    echo "$gcp_path"
  fi
}

# Function to fix common schema issues
fix_schema() {
  local schema="$1"

  if [ "$AUTO_FIX_TYPES" != "true" ]; then
    echo "$schema"
    return
  fi

  # Create a Python script to intelligently fix the schema
  python3 - <<EOF
import json
import sys
import re

schema = '''$schema'''
fields = json.loads(schema)

def infer_better_type(field):
    """Try to infer better types based on field names and patterns"""
    name = field.get('name', '').lower()
    current_type = field.get('type', 'STRING')

    # Already non-string, keep it
    if current_type != 'STRING':
        return field

    # Define patterns for type inference
    patterns = {
        'INTEGER': {
            'exact': ['id', 'count', 'quantity', 'qty', 'number', 'num', 'size', 'age', 'year', 'month', 'day', 'week', 'hour', 'minute', 'second'],
            'suffix': ['_id', '_count', '_num', '_qty', '_size', '_age', '_year', '_month', '_day'],
            'prefix': ['num_', 'count_', 'total_', 'max_', 'min_'],
            'regex': [r'.*_id$', r'^id_', r'.*_key$', r'.*_code$']
        },
        'FLOAT64': {
            'exact': ['price', 'cost', 'amount', 'rate', 'ratio', 'percentage', 'percent', 'score', 'weight', 'height', 'latitude', 'longitude', 'lat', 'lon', 'lng'],
            'suffix': ['_price', '_cost', '_amount', '_rate', '_ratio', '_percent', '_percentage', '_score', '_weight', '_height'],
            'prefix': ['price_', 'cost_', 'amount_', 'rate_', 'total_amount', 'total_cost'],
            'contains': ['amount', 'price', 'cost', 'revenue', 'profit', 'margin']
        },
        'TIMESTAMP': {
            'exact': ['timestamp', 'created_at', 'updated_at', 'modified_at', 'deleted_at', 'last_modified', 'last_updated'],
            'suffix': ['_timestamp', '_at', '_time', '_datetime'],
            'prefix': ['timestamp_', 'ts_'],
            'contains': ['timestamp', '_at', 'datetime']
        },
        'DATE': {
            'exact': ['date', 'birth_date', 'start_date', 'end_date', 'created_date', 'updated_date'],
            'suffix': ['_date', '_dt'],
            'prefix': ['date_', 'dt_'],
            'contains': ['_date', 'date_']
        },
        'BOOLEAN': {
            'exact': ['active', 'enabled', 'deleted', 'valid', 'verified', 'completed', 'success', 'failed'],
            'prefix': ['is_', 'has_', 'can_', 'should_', 'will_', 'was_', 'are_'],
            'suffix': ['_flag', '_active', '_enabled', '_deleted']
        }
    }

    # Check each type pattern
    for new_type, type_patterns in patterns.items():
        # Skip TIMESTAMP check if already matched DATE
        if new_type == 'TIMESTAMP' and field.get('type') == 'DATE':
            continue

        # Check exact matches
        if name in type_patterns.get('exact', []):
            field['type'] = new_type
            field['mode'] = field.get('mode', 'NULLABLE')
            print(f"  Converting {field.get('name')}: STRING -> {new_type} (exact match)", file=sys.stderr)
            return field

        # Check prefix matches
        for prefix in type_patterns.get('prefix', []):
            if name.startswith(prefix):
                field['type'] = new_type
                field['mode'] = field.get('mode', 'NULLABLE')
                print(f"  Converting {field.get('name')}: STRING -> {new_type} (prefix match: {prefix})", file=sys.stderr)
                return field

        # Check suffix matches
        for suffix in type_patterns.get('suffix', []):
            if name.endswith(suffix):
                field['type'] = new_type
                field['mode'] = field.get('mode', 'NULLABLE')
                print(f"  Converting {field.get('name')}: STRING -> {new_type} (suffix match: {suffix})", file=sys.stderr)
                return field

        # Check contains matches
        for contains in type_patterns.get('contains', []):
            if contains in name:
                field['type'] = new_type
                field['mode'] = field.get('mode', 'NULLABLE')
                print(f"  Converting {field.get('name')}: STRING -> {new_type} (contains: {contains})", file=sys.stderr)
                return field

        # Check regex patterns
        for pattern in type_patterns.get('regex', []):
            if re.match(pattern, name):
                field['type'] = new_type
                field['mode'] = field.get('mode', 'NULLABLE')
                print(f"  Converting {field.get('name')}: STRING -> {new_type} (regex match)", file=sys.stderr)
                return field

    return field

fixed_fields = [infer_better_type(field) for field in fields]
print(json.dumps(fixed_fields, indent=2))
EOF
}

# Process a single parquet path and extract schema
process_parquet_path() {
  local gcp_path="$1"

  log "Processing path: $gcp_path"

  # Get table name
  local table_name=$(infer_table_name "$gcp_path")
  log "  Table name: $table_name"

  # Check if schema already exists
  if [ -f "${SCHEMA_DIR}/${table_name}.json" ]; then
    log "  Schema already exists: ${SCHEMA_DIR}/${table_name}.json (skipping)"
    return 0
  fi

  # Get first parquet file if path contains wildcard
  local parquet_file=$(get_first_parquet "$gcp_path")
  log "  Using file: $parquet_file"

  # Generate temporary table name
  local temp_table="schema_extract_${table_name}_$(date +%s)"

  # Create temporary dataset if it doesn't exist
  if ! bq --project_id="$BQ_PROJECT" show --dataset "$TEMP_DATASET" >/dev/null 2>&1; then
    log "  Creating temporary dataset: $TEMP_DATASET"
    bq --project_id="$BQ_PROJECT" mk \
      --dataset \
      --location="$BQ_LOCATION" \
      "${BQ_PROJECT}:${TEMP_DATASET}" >/dev/null 2>&1
  fi

  # Load a sample of the data to infer schema
  log "  Loading sample data to infer schema..."
  if ! bq --project_id="$BQ_PROJECT" load \
    --source_format=PARQUET \
    --max_bad_records=1000 \
    --autodetect \
    --max_rows_per_request="$SAMPLE_ROWS" \
    "${BQ_PROJECT}:${TEMP_DATASET}.${temp_table}" \
    "$parquet_file" >/dev/null 2>&1; then
    log "  ERROR: Failed to load sample data from $parquet_file"
    return 1
  fi

  # Extract the schema
  local schema=$(bq --project_id="$BQ_PROJECT" show \
    --schema \
    --format=prettyjson \
    "${BQ_PROJECT}:${TEMP_DATASET}.${temp_table}" 2>/dev/null)

  if [ -z "$schema" ]; then
    log "  ERROR: Failed to extract schema"
    bq --project_id="$BQ_PROJECT" rm -f "${BQ_PROJECT}:${TEMP_DATASET}.${temp_table}" 2>/dev/null || true
    return 1
  fi

  # Apply fixes to the schema if enabled
  local fixed_schema
  if [ "$AUTO_FIX_TYPES" = "true" ]; then
    log "  Applying type inference..."
    fixed_schema=$(fix_schema "$schema" 2>/dev/null)
  else
    fixed_schema="$schema"
  fi

  # Save the schema
  local output_file="${SCHEMA_DIR}/${table_name}.json"
  echo "$fixed_schema" > "$output_file"
  log "  Schema saved to: $output_file"

  # Show summary
  local field_count=$(echo "$fixed_schema" | python3 -c "import json, sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "?")
  log "  Total fields: $field_count"

  # Clean up temporary table
  bq --project_id="$BQ_PROJECT" rm -f "${BQ_PROJECT}:${TEMP_DATASET}.${temp_table}" 2>/dev/null || true

  return 0
}

# --- Main execution ----------------------------------------------------------

# Validate prerequisites
command -v bq >/dev/null 2>&1 || { echo "Error: bq CLI not found. Please install Google Cloud SDK"; exit 1; }
command -v gsutil >/dev/null 2>&1 || { echo "Error: gsutil not found. Please install Google Cloud SDK"; exit 1; }
[ -n "$BQ_PROJECT" ] || { echo "Error: BQ_PROJECT not set and no default project configured"; exit 1; }

# Create schema directory if it doesn't exist
mkdir -p "$SCHEMA_DIR"

echo "===== BigQuery Schema Extractor ====="
echo "Configuration:"
echo "  Project: $BQ_PROJECT"
echo "  Location: $BQ_LOCATION"
echo "  Schema directory: $SCHEMA_DIR"
echo "  Auto-fix types: $AUTO_FIX_TYPES"
echo "  Sample rows: $SAMPLE_ROWS"
echo ""

# Determine what to process
if [ -n "$SINGLE_PATH" ]; then
  # Process single path from command line
  echo "Processing single path from command line"
  echo ""
  process_parquet_path "$SINGLE_PATH"
else
  # Process all paths from INPUT_FILE
  if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file not found: $INPUT_FILE"
    echo ""
    echo "Usage:"
    echo "  $0                           # Process all paths in $INPUT_FILE"
    echo "  $0 <gs://path/to/file>       # Process single path"
    exit 1
  fi

  echo "Processing paths from: $INPUT_FILE"
  echo ""

  # Read paths from input file
  PATHS_TO_PROCESS=()
  while IFS= read -r line; do
    # Skip comments and empty lines
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue

    # Trim whitespace
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    if [ -n "$line" ]; then
      PATHS_TO_PROCESS+=("$line")
    fi
  done < "$INPUT_FILE"

  if [ "${#PATHS_TO_PROCESS[@]}" -eq 0 ]; then
    echo "No valid paths found in $INPUT_FILE"
    exit 0
  fi

  echo "Found ${#PATHS_TO_PROCESS[@]} paths to process"
  echo ""

  # Process each path
  SUCCESS_COUNT=0
  FAILED_COUNT=0
  SKIPPED_COUNT=0

  for path in "${PATHS_TO_PROCESS[@]}"; do
    if process_parquet_path "$path"; then
      if [ -f "${SCHEMA_DIR}/$(infer_table_name "$path").json" ]; then
        ((SUCCESS_COUNT++))
      else
        ((SKIPPED_COUNT++))
      fi
    else
      ((FAILED_COUNT++))
    fi
    echo ""
  done

  # Summary
  echo "===== Schema Extraction Complete ====="
  echo "Summary:"
  echo "  Successful: $SUCCESS_COUNT"
  echo "  Skipped (already exists): $SKIPPED_COUNT"
  echo "  Failed: $FAILED_COUNT"
  echo ""
  echo "Schema files in $SCHEMA_DIR:"
  ls -la "$SCHEMA_DIR"/*.json 2>/dev/null || echo "  No schema files found"
fi

echo ""
echo "You can now use these schemas with the loader:"
echo "  SCHEMA_DIR=$SCHEMA_DIR ./load_gcp_paths_to_bq.sh"