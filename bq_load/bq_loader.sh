#!/usr/bin/env bash
#
# load_parquet_typed.sh - Load all-string Parquet files with proper typing
#
set -euo pipefail

# Load environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loading environment from $SCRIPT_DIR/.env"
  source "$SCRIPT_DIR/.env"
fi

# Configuration
INPUT_FILE="${INPUT_FILE:-bq_source.txt}"
BQ_PROJECT="${BQ_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-cq_source}"
BQ_LOCATION="${BQ_LOCATION:-US}"
SCHEMA_DIR="${SCHEMA_DIR:-schemas}"
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

# Table name inference
infer_table_name() {
  local gcp_path="$1"
  local path_only="${gcp_path#gs://}"
  path_only="${path_only#*/}"

  if [[ "$path_only" =~ ^(.*)/\*\.[^/]+$ ]]; then
    path_only="${BASH_REMATCH[1]}"
  elif [[ "$path_only" =~ ^(.*)/[^/]+\.[^/]+$ ]]; then
    path_only="${BASH_REMATCH[1]}"
  fi

  IFS='/' read -ra PATH_PARTS <<< "$path_only"

  local table_name=""
  for i in "${!PATH_PARTS[@]}"; do
    if [[ "${PATH_PARTS[$i]}" =~ ^[0-9]{8}$ ]]; then
      if [ $i -gt 0 ]; then
        table_name="${PATH_PARTS[$((i-1))]}"
        break
      fi
    fi
  done

  if [ -z "$table_name" ]; then
    local num_parts=${#PATH_PARTS[@]}
    if [ $num_parts -gt 0 ]; then
      table_name="${PATH_PARTS[$((num_parts-1))]}"
    fi
  fi

  table_name=$(echo "$table_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')

  if [ -z "$table_name" ]; then
    table_name="unknown_table_$(date +%s)"
  fi

  echo "$table_name"
}

# Generate SQL transformation
generate_sql() {
  local schema_file="$1"
  local staging_table="$2"
  local final_table="$3"
  local sql_file="$4"

  cat > "$WORK_DIR/generate_sql.py" << 'PYTHON_SCRIPT'
import json
import sys

schema_file = sys.argv[1]
staging_table = sys.argv[2]
final_table = sys.argv[3]

with open(schema_file, 'r') as f:
    schema = json.load(f)

print(f"CREATE OR REPLACE TABLE `{final_table}` AS")
print("SELECT")

for i, field in enumerate(schema):
    col_name = field['name']
    bq_type = field['type']

    if bq_type in ['INTEGER', 'INT64']:
        cast_expr = f"""  CASE
    WHEN {col_name} IS NULL OR {col_name} = 'NULL' OR {col_name} = '' THEN NULL
    ELSE SAFE_CAST(REGEXP_REPLACE({col_name}, r'[^0-9-]', '') AS INT64)
  END AS {col_name}"""

    elif bq_type in ['FLOAT', 'FLOAT64']:
        cast_expr = f"""  CASE
    WHEN {col_name} IS NULL OR {col_name} = 'NULL' OR {col_name} = '' THEN NULL
    ELSE SAFE_CAST(REGEXP_REPLACE({col_name}, r'[^0-9.-]', '') AS FLOAT64)
  END AS {col_name}"""

    elif bq_type == 'BOOLEAN':
        cast_expr = f"""  CASE
    WHEN UPPER({col_name}) IN ('TRUE', 'T', 'YES', 'Y', '1') THEN TRUE
    WHEN UPPER({col_name}) IN ('FALSE', 'F', 'NO', 'N', '0') THEN FALSE
    ELSE NULL
  END AS {col_name}"""

    elif bq_type == 'TIMESTAMP':
        cast_expr = f"""  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', {col_name}),
    SAFE.PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%S', {col_name}),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', {col_name})
  ) AS {col_name}"""

    elif bq_type == 'DATE':
        cast_expr = f"""  CASE
    WHEN {col_name} IS NULL OR {col_name} = 'NULL' OR {col_name} = '' THEN NULL
    WHEN REGEXP_CONTAINS({col_name}, r'^[0-9]{{4}}\\.?0?$') THEN
      SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(REGEXP_EXTRACT({col_name}, r'^([0-9]{{4}})'), '-01-01'))
    WHEN REGEXP_CONTAINS({col_name}, r'^[0-9]{{8}}$') THEN
      SAFE.PARSE_DATE('%Y%m%d', {col_name})
    ELSE COALESCE(
      SAFE.PARSE_DATE('%Y-%m-%d', {col_name}),
      SAFE.PARSE_DATE('%Y/%m/%d', {col_name}),
      SAFE.PARSE_DATE('%m/%d/%Y', {col_name}),
      SAFE.PARSE_DATE('%d/%m/%Y', {col_name})
    )
  END AS {col_name}"""

    else:
        cast_expr = f"""  CASE
    WHEN {col_name} = 'NULL' THEN NULL
    ELSE {col_name}
  END AS {col_name}"""

    if i < len(schema) - 1:
        cast_expr += ","

    print(cast_expr)

print(f"FROM `{staging_table}`")
PYTHON_SCRIPT

  python3 "$WORK_DIR/generate_sql.py" "$schema_file" "$staging_table" "$final_table" > "$sql_file"
}

# Process a single path
process_path() {
  local gcp_path="$1"
  local table_name=$(infer_table_name "$gcp_path")

  info "Processing: $gcp_path"
  info "  Table name: $table_name"

  local staging_table="${table_name}_staging"
  local staging_table_ref="${BQ_PROJECT}:${STAGING_DATASET}.${staging_table}"
  local final_table_ref="${BQ_PROJECT}:${BQ_DATASET}.${table_name}"
  local schema_file="${SCHEMA_DIR}/${table_name}.json"

  if [ ! -f "$schema_file" ]; then
    warn "  Schema file not found: $schema_file"
    return 1
  fi

  # Step 1: Load to staging
  info "  Loading to staging: $staging_table_ref"
  if ! bq --quiet --project_id="$BQ_PROJECT" load \
    --source_format=PARQUET \
    --autodetect \
    --replace \
    "${staging_table_ref}" \
    "$gcp_path" 2>&1 | tee -a "$LOG_FILE"; then
    error "  Failed to load to staging"
    return 1
  fi

  # Step 2: Generate and execute SQL
  info "  Generating transformation SQL..."
  local sql_file="$WORK_DIR/${table_name}.sql"
  generate_sql "$schema_file" \
    "${BQ_PROJECT}.${STAGING_DATASET}.${staging_table}" \
    "${BQ_PROJECT}.${BQ_DATASET}.${table_name}" \
    "$sql_file"

  info "  Executing transformation..."
  if ! bq --quiet --project_id="$BQ_PROJECT" query \
    --use_legacy_sql=false \
    --format=none < "$sql_file" 2>&1 | tee -a "$LOG_FILE"; then
    error "  Transformation failed"
    return 1
  fi

  # Step 3: Verify
  local row_count=$(bq --project_id="$BQ_PROJECT" query \
    --use_legacy_sql=false \
    --format=csv \
    "SELECT COUNT(*) FROM \`${BQ_PROJECT}.${BQ_DATASET}.${table_name}\`" 2>/dev/null | tail -1)
  info "  Loaded $row_count rows"

  # Step 4: Clean up staging (optional)
  if [ "$KEEP_STAGING" != "true" ]; then
    info "  Cleaning up staging table..."
    bq --quiet --project_id="$BQ_PROJECT" rm -f "$staging_table_ref" 2>/dev/null || true
  else
    info "  Keeping staging table: $staging_table_ref"
  fi

  return 0
}

# Main
main() {
  info "===== Parquet Typed Loader Started ====="
  info "Configuration:"
  info "  Project: $BQ_PROJECT"
  info "  Dataset: $BQ_DATASET"
  info "  Staging: $STAGING_DATASET"
  info "  Schema Dir: $SCHEMA_DIR"
  info "  Keep Staging: $KEEP_STAGING"

  # Validate
  command -v bq >/dev/null 2>&1 || die "bq CLI not found"
  [ -n "$BQ_PROJECT" ] || die "BQ_PROJECT not set"
  [ -f "$INPUT_FILE" ] || die "Input file not found: $INPUT_FILE"
  [ -d "$SCHEMA_DIR" ] || die "Schema directory not found: $SCHEMA_DIR"

  # Create datasets
  for dataset in "$BQ_DATASET" "$STAGING_DATASET"; do
    if ! bq --quiet --project_id="$BQ_PROJECT" show --dataset "$dataset" >/dev/null 2>&1; then
      info "Creating dataset: $dataset"
      bq --quiet --project_id="$BQ_PROJECT" mk \
        --dataset --location="$BQ_LOCATION" \
        "${BQ_PROJECT}:${dataset}" || die "Failed to create dataset"
    fi
  done

  # Process paths
  SUCCESS=0
  FAILED=0

  while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue

    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    if [ -n "$line" ]; then
      if process_path "$line"; then
        ((SUCCESS++))
      else
        ((FAILED++))
      fi
    fi
  done < "$INPUT_FILE"

  info "===== Complete ====="
  info "Success: $SUCCESS, Failed: $FAILED"

  [ "$FAILED" -eq 0 ] || exit 1
}

# Run
main "$@"