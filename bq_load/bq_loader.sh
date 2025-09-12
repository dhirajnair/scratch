#!/usr/bin/env bash
#
# load_parquet_typed.sh - Load Parquet files and transform to proper types
#
# Process:
# 1. ALWAYS drops existing final and staging tables (prevents schema conflicts)
# 2. Loads Parquet to staging with autodetect (handles INT96 timestamps)
# 3. Transforms to final table using SAFE_CAST (works with any source type)
# 4. Uses CREATE OR REPLACE as additional safety net
#
# Key features:
# - No failures from existing tables or schema mismatches
# - Handles INT96 timestamps in Parquet (legacy format)
# - Works with YYYYMMDD date formats (20250331 → 2025-03-31)
# - No PARSE_TIMESTAMP (which fails on non-STRING columns)
# - Clean slate approach ensures reliability
#
# Environment Variables:
#   KEEP_STAGING=true  - Keep staging tables after processing (default: false)
#
set -euo pipefail

# Load environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loading environment from $SCRIPT_DIR/.env"
  source "$SCRIPT_DIR/.env"
fi

# Configuration
INPUT_FILE="${INPUT_FILE:-boaredex_input.txt}"
BQ_PROJECT="${BQ_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-cq_source}"
BQ_LOCATION="${BQ_LOCATION:-US}"
STAGING_DATASET="${STAGING_DATASET:-${BQ_DATASET}_staging}"
KEEP_STAGING="${KEEP_STAGING:-false}"

# Timestamp and logging
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
WORK_DIR="/tmp/typed_loader_$$"
mkdir -p "$WORK_DIR"
LOG_FILE="$WORK_DIR/typed_loader_${TIMESTAMP}.log"

# Logging functions
log() {
  local level="$1"
  shift
  echo "$(date --iso-8601=seconds) [${level}] $*" | tee -a "$LOG_FILE"
}

info() { log "INFO" "$@"; }
warn() { log "WARN" "$@"; }
error() { log "ERROR" "$@"; }
die() { error "$@"; exit 1; }

# Table name inference from schema file path
infer_table_name_from_schema() {
  local schema_path="$1"

  # Extract filename from GCS path
  local filename=$(basename "$schema_path")

  # Remove .json extension
  local table_name="${filename%.json}"

  # Clean up the table name (lowercase, replace invalid chars with underscore)
  table_name=$(echo "$table_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')

  if [ -z "$table_name" ]; then
    table_name="unknown_table_$(date +%s)"
  fi

  echo "$table_name"
}

# Download schema from GCS
download_schema() {
  local gcs_schema_path="$1"
  local local_schema_path="$2"

  info "  Downloading schema from: $gcs_schema_path"

  if ! gsutil cp "$gcs_schema_path" "$local_schema_path" </dev/null 2>&1 | tee -a "$LOG_FILE"; then
    error "  Failed to download schema"
    return 1
  fi

  return 0
}

# Generate SQL transformation - Uses SAFE_CAST for all conversions
# SAFE_CAST works with any source column type (STRING, TIMESTAMP, INT96, etc.)
# No need for PARSE_TIMESTAMP which only works on STRING columns.
generate_sql() {
  local schema_file="$1"
  local staging_table="$2"
  local final_table="$3"
  local sql_file="$4"
  local staging_table_ref="$5"

  # First get the staging table schema
  local staging_schema_file="$WORK_DIR/staging_schema_temp.json"
  if ! bq --project_id="$BQ_PROJECT" show --schema --format=prettyjson "$staging_table_ref" > "$staging_schema_file" 2>>"$LOG_FILE"; then
    error "  Failed to get staging table schema"
    return 1
  fi

  cat > "$WORK_DIR/generate_sql.py" << 'PYTHON_SCRIPT'
import json
import sys

# This script generates SQL that uses SAFE_CAST for all type conversions.
# SAFE_CAST works with any source column type (STRING, TIMESTAMP, INT96, etc.)
# This avoids issues with PARSE_TIMESTAMP which only works on STRING columns.

schema_file = sys.argv[1]
staging_table = sys.argv[2]
final_table = sys.argv[3]
staging_schema_file = sys.argv[4]

with open(schema_file, 'r') as f:
    schema = json.load(f)

with open(staging_schema_file, 'r') as f:
    staging_schema = json.load(f)

# Create sets for comparison
expected_columns = {field['name'].lower() for field in schema}
staging_columns = {field['name'].lower(): field for field in staging_schema}

# Find extra columns
extra_columns = set(staging_columns.keys()) - expected_columns

# Check for non-string extra columns
extra_non_string = []
extra_string = []
for col in extra_columns:
    col_type = staging_columns[col].get('type', '').upper()
    if col_type != 'STRING':
        extra_non_string.append(f"{col}({col_type})")
    else:
        extra_string.append(col)

if extra_non_string:
    print(f"ERROR: Found non-STRING extra columns: {', '.join(extra_non_string)}", file=sys.stderr)
    sys.exit(1)

if extra_string:
    print(f"INFO: Including extra STRING columns: {', '.join(sorted(extra_string))}", file=sys.stderr)

# We drop the table before this, but use CREATE OR REPLACE as a safety net
print(f"CREATE OR REPLACE TABLE `{final_table}` AS")
print("SELECT")

# Process schema columns
for i, field in enumerate(schema):
    col_name = field['name']
    bq_type = field['type'].upper()

    # SIMPLIFIED: Use SAFE_CAST for everything - it works whether source is STRING or already typed

    if bq_type in ['INTEGER', 'INT64']:
        # For integers, clean up potential non-numeric characters
        cast_expr = f"""  SAFE_CAST(
    CASE
      WHEN {col_name} IS NULL THEN NULL
      WHEN CAST({col_name} AS STRING) IN ('NULL', '') THEN NULL
      ELSE REGEXP_REPLACE(CAST({col_name} AS STRING), r'[^0-9-]', '')
    END AS INT64
  ) AS {col_name}"""

    elif bq_type in ['FLOAT', 'FLOAT64']:
        # For floats, clean up potential non-numeric characters
        cast_expr = f"""  SAFE_CAST(
    CASE
      WHEN {col_name} IS NULL THEN NULL
      WHEN CAST({col_name} AS STRING) IN ('NULL', '') THEN NULL
      ELSE REGEXP_REPLACE(CAST({col_name} AS STRING), r'[^0-9.-]', '')
    END AS FLOAT64
  ) AS {col_name}"""

    elif bq_type in ['BOOLEAN', 'BOOL']:
        # For booleans, handle various representations
        cast_expr = f"""  CASE
    WHEN {col_name} IS NULL THEN NULL
    WHEN UPPER(CAST({col_name} AS STRING)) IN ('TRUE', 'T', 'YES', 'Y', '1') THEN TRUE
    WHEN UPPER(CAST({col_name} AS STRING)) IN ('FALSE', 'F', 'NO', 'N', '0') THEN FALSE
    ELSE NULL
  END AS {col_name}"""

    elif bq_type == 'TIMESTAMP':
        # SIMPLE SAFE_CAST - NO PARSE_TIMESTAMP!
        cast_expr = f"  SAFE_CAST({col_name} AS TIMESTAMP) AS {col_name}"

    elif bq_type == 'DATE':
        # Handle various date formats including YYYYMMDD
        cast_expr = f"""  CASE
    WHEN {col_name} IS NULL THEN NULL
    WHEN CAST({col_name} AS STRING) IN ('NULL', '') THEN NULL
    -- Handle YYYYMMDD format (8 digits)
    WHEN REGEXP_CONTAINS(CAST({col_name} AS STRING), r'^[0-9]{{8}}$') THEN
      SAFE.PARSE_DATE('%Y%m%d', CAST({col_name} AS STRING))
    -- Handle YYYY-MM-DD and other standard formats
    ELSE SAFE_CAST({col_name} AS DATE)
  END AS {col_name}"""

    elif bq_type == 'STRING':
        # For strings, just handle NULL literal
        cast_expr = f"""  CASE
    WHEN CAST({col_name} AS STRING) = 'NULL' THEN NULL
    ELSE CAST({col_name} AS STRING)
  END AS {col_name}"""

    else:
        # For any other type, just use SAFE_CAST
        cast_expr = f"  SAFE_CAST({col_name} AS {bq_type}) AS {col_name}"

    # Add comma if not last field or there are extra columns
    if i < len(schema) - 1 or extra_string:
        cast_expr += ","

    print(cast_expr)

# Add extra string columns
for i, col_name in enumerate(sorted(extra_string)):
    cast_expr = f"  {col_name}"
    if i < len(extra_string) - 1:
        cast_expr += ","
    print(cast_expr)

print(f"FROM `{staging_table}`")
PYTHON_SCRIPT

  # Run Python script and capture stderr
  local python_stderr="$WORK_DIR/python_stderr.txt"
  if python3 "$WORK_DIR/generate_sql.py" "$schema_file" "$staging_table" "$final_table" "$staging_schema_file" > "$sql_file" 2>"$python_stderr"; then
    # Log any info messages
    if [ -s "$python_stderr" ]; then
      grep "^INFO:" "$python_stderr" | sed 's/^INFO: /  /' | while IFS= read -r line; do
        info "$line"
      done
    fi
    rm -f "$staging_schema_file"
    return 0
  else
    # Show error and fail
    if [ -s "$python_stderr" ]; then
      grep "^ERROR:" "$python_stderr" | sed 's/^ERROR: /  /' | while IFS= read -r line; do
        error "$line"
      done
    fi
    rm -f "$staging_schema_file"
    return 1
  fi
}

# Process a single path with schema
process_path_with_schema() {
  local gcp_path="$1"
  local gcs_schema_path="$2"

  # Infer table name from schema file name
  local table_name=$(infer_table_name_from_schema "$gcs_schema_path")

  info "Processing: $gcp_path"
  info "  Schema: $gcs_schema_path"
  info "  Table name: $table_name"

  # Download schema to local temp directory
  local local_schema_file="$WORK_DIR/${table_name}_schema.json"
  if ! download_schema "$gcs_schema_path" "$local_schema_file"; then
    error "  Failed to download schema from GCS"
    return 1
  fi

  local staging_table="${table_name}_staging"
  local staging_table_ref="${BQ_PROJECT}:${STAGING_DATASET}.${staging_table}"
  local final_table_ref="${BQ_PROJECT}:${BQ_DATASET}.${table_name}"

  # Step 0: Drop existing tables to ensure clean slate (prevents all conflicts)
  info "  Ensuring clean slate - dropping any existing tables..."

  # Drop final table/view if exists (rm -f handles both and won't error if not exists)
  info "  Dropping final table/view (if exists): $final_table_ref"
  bq --quiet --project_id="$BQ_PROJECT" rm -f "$final_table_ref" </dev/null 2>/dev/null || true

  # Drop staging table if exists
  info "  Dropping staging table (if exists): $staging_table_ref"
  bq --quiet --project_id="$BQ_PROJECT" rm -f "$staging_table_ref" </dev/null 2>/dev/null || true

  info "  ✓ Clean slate ensured - no existing tables"

  # Step 1: Load to staging
  # Always use autodetect since our SAFE_CAST SQL handles any column type
  info "  Loading to staging: $staging_table_ref"
  info "  Using autodetect to preserve Parquet native types (INT96 timestamps, etc.)"

  if ! bq --quiet --project_id="$BQ_PROJECT" load \
    --source_format=PARQUET \
    --autodetect \
    --replace \
    "${staging_table_ref}" \
    "$gcp_path" </dev/null 2>&1 | tee -a "$LOG_FILE"; then
    error "  Failed to load to staging"
    return 1
  fi

  info "  Successfully loaded to staging with autodetect"

  # Step 2: Generate and execute SQL
  info "  Generating transformation SQL..."
  local sql_file="$WORK_DIR/${table_name}.sql"

  if ! generate_sql "$local_schema_file" \
    "${BQ_PROJECT}.${STAGING_DATASET}.${staging_table}" \
    "${BQ_PROJECT}.${BQ_DATASET}.${table_name}" \
    "$sql_file" \
    "$staging_table_ref"; then
    error "  Failed to generate SQL"
    return 1
  fi

  info "  Executing transformation (CREATE OR REPLACE)..."
  if ! bq --quiet --project_id="$BQ_PROJECT" query \
    --use_legacy_sql=false \
    --format=none < "$sql_file" 2>&1 | tee -a "$LOG_FILE"; then
    error "  Transformation failed"
    error "  Check the generated SQL at: $sql_file"
    # Save SQL for debugging even after cleanup
    cp "$sql_file" "/tmp/${table_name}_failed.sql" 2>/dev/null || true
    error "  SQL saved to: /tmp/${table_name}_failed.sql"
    return 1
  fi

  # Step 3: Verify
  local row_count=$(bq --project_id="$BQ_PROJECT" query \
    --use_legacy_sql=false \
    --format=csv \
    "SELECT COUNT(*) FROM \`${BQ_PROJECT}.${BQ_DATASET}.${table_name}\`" </dev/null 2>/dev/null | tail -1)
  info "  ✓ Success! Loaded $row_count rows to final table: $table_name"

  # Step 4: Clean up staging (optional - only matters if KEEP_STAGING=true)
  if [ "$KEEP_STAGING" = "true" ]; then
    info "  Keeping staging table for debugging: $staging_table_ref"
  else
    # With clean slate approach, staging will be dropped at next run anyway
    # But let's clean it up to save space
    bq --quiet --project_id="$BQ_PROJECT" rm -f "$staging_table_ref" </dev/null 2>/dev/null || true
    info "  Staging table cleaned up"
  fi

  # Clean up local schema file
  rm -f "$local_schema_file"

  # Success!
  info "  ✓ Table processing complete: ${table_name}"
  info ""
  return 0
}

# Main - FIXED WITH ARRAY-BASED READING
main() {
  info "=========================================="
  info "     Parquet Typed Loader v2.0"
  info "=========================================="
  info "Configuration:"
  info "  Project:      $BQ_PROJECT"
  info "  Final Dataset: $BQ_DATASET"
  info "  Staging:      $STAGING_DATASET"
  info "  Input File:   $INPUT_FILE"
  info "  Keep Staging: $KEEP_STAGING"
  info "  Strategy:     Drop & Recreate (no conflicts)"
  info "  Log File:     $LOG_FILE"
  info "=========================================="

  # Validate
  command -v bq >/dev/null 2>&1 || die "bq CLI not found"
  command -v gsutil >/dev/null 2>&1 || die "gsutil CLI not found"
  [ -n "$BQ_PROJECT" ] || die "BQ_PROJECT not set"
  [ -f "$INPUT_FILE" ] || die "Input file not found: $INPUT_FILE"

  # Create datasets (ignore if already exists)
  for dataset in "$BQ_DATASET" "$STAGING_DATASET"; do
    if ! bq --quiet --project_id="$BQ_PROJECT" show --dataset "$dataset" </dev/null >/dev/null 2>&1; then
      info "Creating dataset: $dataset"
      if ! bq --quiet --project_id="$BQ_PROJECT" mk \
        --dataset --location="$BQ_LOCATION" \
        "${BQ_PROJECT}:${dataset}" </dev/null >/dev/null 2>&1; then
        # Check if error is because dataset already exists
        if bq --quiet --project_id="$BQ_PROJECT" show --dataset "$dataset" </dev/null >/dev/null 2>&1; then
          info "  Dataset already exists: $dataset"
        else
          die "Failed to create dataset: $dataset"
        fi
      fi
    else
      info "Dataset exists: $dataset"
    fi
  done

  # Read all valid lines into an array
  declare -a VALID_LINES=()

  while IFS= read -r line; do
    # Skip comments and empty lines
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue

    # Trim whitespace
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    # Add to array if not empty
    if [ -n "$line" ]; then
      VALID_LINES+=("$line")
    fi
  done < "$INPUT_FILE"

  # Get total count
  TOTAL="${#VALID_LINES[@]}"

  info "Found $TOTAL tables to process"
  info ""

  # Process entries from array
  SUCCESS=0
  FAILED=0

  for ((idx=0; idx<${#VALID_LINES[@]}; idx++)); do
    line="${VALID_LINES[$idx]}"
    CURRENT=$((idx + 1))

    info "========== Processing table $CURRENT of $TOTAL =========="

    # Parse comma-separated values
    IFS=',' read -r gcp_path gcs_schema_path <<< "$line"

    # Trim whitespace from each field
    gcp_path=$(echo "$gcp_path" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    gcs_schema_path=$(echo "$gcs_schema_path" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    if [ -z "$gcp_path" ] || [ -z "$gcs_schema_path" ]; then
      warn "Skipping invalid line $CURRENT (line content: '$line')"
      warn "  Expected format: gcp_path,gcs_schema_path"
      warn "  Got: gcp_path='$gcp_path', gcs_schema_path='$gcs_schema_path'"
      ((FAILED++)) || true
      continue
    fi

    if process_path_with_schema "$gcp_path" "$gcs_schema_path" </dev/null; then
      ((SUCCESS++)) || true
      info "✓ Completed table $CURRENT of $TOTAL"
    else
      ((FAILED++)) || true
      error "✗ Failed table $CURRENT of $TOTAL"
    fi

    info ""
  done

  # Clean up work directory (keep if there were failures for debugging)
  if [ "$FAILED" -eq 0 ]; then
    rm -rf "$WORK_DIR"
    info "Cleaned up temporary files"
  else
    info "Keeping work directory for debugging: $WORK_DIR"
  fi

  info "===== Processing Complete ====="
  info "✓ Success: $SUCCESS tables"
  if [ "$FAILED" -gt 0 ]; then
    error "✗ Failed: $FAILED tables (check logs for details)"
  fi
  info "Dataset: ${BQ_PROJECT}:${BQ_DATASET}"

  [ "$FAILED" -eq 0 ] || exit 1
}

# Run
main "$@"