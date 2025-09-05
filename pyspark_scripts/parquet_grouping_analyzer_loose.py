### differnt version
#!/usr/bin/env python3
"""
ADD EXCEL SUPPORT !!!!! TODO
Parquet Grouping Analyzer for Azure Blob Storage (Databricks single-user cluster)

This is a patched full-file version incorporating focused, minimal optimizations:
- Uses dbutils.fs.ls for fast file discovery (Databricks)
- Avoids full-file inferSchema/count where possible (sample-based)
- Safer Spark session tuning for single-user clusters
- Robust schema fingerprinting
- Excel fallback using pandas when spark-excel is not available
- Safer save to blob using dbutils.fs.put for small reports
- Uses environment variables / dbutils.secrets for credentials (no hardcoding)

Drop into a Databricks notebook or run on a single-user cluster. Adjust
SECRET SCOPE / KEYS and environment variables as needed.

Requirements:
    - pyspark
    - pandas
    - openpyxl (optional, for Excel fallback)
    - xlrd (optional, legacy Excel)
"""

import os
import re
import json
import uuid
import time
import gc
import hashlib
import logging
import datetime
import traceback
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional

from pyspark.sql import SparkSession
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io

EXCEL_SUPPORT_AVAILABLE = True


# Try to reference dbutils (Databricks). If not present, provide safe fallback.
try:
    dbutils  # type: ignore
except NameError:  # pragma: no cover
    class _DBUtilsFallback:
        def ls(self, path):
            raise RuntimeError("dbutils is not available in this environment.")
        def fs(self):  # pragma: no cover
            raise RuntimeError("dbutils.fs is not available.")
        def secrets(self):  # pragma: no cover
            raise RuntimeError("dbutils.secrets is not available.")
        def put(self, *args, **kwargs):  # pragma: no cover
            raise RuntimeError("dbutils.fs.put is not available.")
    dbutils = _DBUtilsFallback()  # type: ignore

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================
MAX_SAMPLE_ROWS = 5                           # Number of top records to analyze per file
SUPPORTED_EXTENSIONS = [".csv"]               # CSV file extensions
EXCEL_EXTENSIONS = [".xlsx", ".xls"]          # Excel file extensions
SIMILARITY_THRESHOLD = 0.8                    # Schema similarity threshold for grouping (0.0-1.0)
INFER_SCHEMA_SAMPLE_ROWS = 1000               # Rows to use for sampling schema inference
DEFAULT_SHUFFLE_PARTITIONS = 8                # sensible default for single-user small cluster
PARQUET_BLOCK_SIZE_BYTES = 128 * 1024 * 1024  # 128MB
REPORT_FILE_SIZE_LIMIT = 8 * 1024 * 1024      # 8MB - small report heuristics

# Module-level output dir placeholder (set in main)
OUTPUT_DIR = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Spark session helper
# =============================================================================
def get_or_create_spark_session():
    """Get or create Spark session with sensible single-user cluster defaults"""
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            logger.info("✓ Using existing Spark session")
            # ensure single-user tuned settings (safe to re-set)
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.shuffle.partitions", str(DEFAULT_SHUFFLE_PARTITIONS))
            spark.conf.set("parquet.block.size", str(PARQUET_BLOCK_SIZE_BYTES))
            return spark

        spark = SparkSession.builder \
            .appName("File_Analyzer_Parquet_Grouping") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        spark.conf.set("spark.sql.shuffle.partitions", str(DEFAULT_SHUFFLE_PARTITIONS))
        spark.conf.set("parquet.block.size", str(PARQUET_BLOCK_SIZE_BYTES))
        logger.info("✓ Created new Spark session with tuned defaults")
        return spark
    except Exception:
        logger.exception("Error creating Spark session")
        raise

# =============================================================================
# FileAnalyzer
# =============================================================================
class FileAnalyzer:
    """Class to analyze files and suggest Parquet groupings"""

    def __init__(self, spark_session):
        self.spark = spark_session
        self.schema_groups = defaultdict(list)
        self.analysis_results = []

    # -------------------------
    # Discovery
    # -------------------------
    def discover_files(self, directory_path: str) -> List[Dict[str, Any]]:
        """
        List files using dbutils.fs.ls (Databricks) and return concrete file entries.
        Expects abfss:// or mounted paths.
        """
        discovered = []
        try:
            # dbutils.fs.ls returns FileInfo objects with .path, .name, .size attributes
            entries = dbutils.fs.ls(directory_path)
            for f in entries:
                name = f.name
                path = f.path
                lower = name.lower()
                if any(lower.endswith(ext) for ext in (SUPPORTED_EXTENSIONS + (EXCEL_EXTENSIONS if EXCEL_SUPPORT_AVAILABLE else []))):
                    size_mb = (f.size / (1024.0 * 1024.0)) if hasattr(f, "size") else 0.0
                    discovered.append({
                        "path": path,
                        "name": name,
                        "extension": Path(name).suffix.lower(),
                        "size": getattr(f, "size", 0),
                        "size_mb": size_mb
                    })
            logger.info(f"Discovered {len(discovered)} supported files in {directory_path}")
            return discovered
        except Exception:
            logger.exception(f"Failed listing files at {directory_path}")
            raise

    # -------------------------
    # CSV reader (sample-based)
    # -------------------------
    def _read_csv_file(self, file_path: str, infer_schema_sample_rows: int = INFER_SCHEMA_SAMPLE_ROWS):
        """
        Read CSV with a cheap sample-based schema inference and limited sampling for analysis.
        Strategy:
          1) Read a limited sample without schema inference (strings) to capture header and examples.
          2) Use that sample's schema (string-based) to read full file avoiding heavy infer on entire file.
        """
        try:
            # Read small sample as strings to capture header and sample rows quickly
            sample_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .csv(file_path).limit(infer_schema_sample_rows)

            # Use the sample's schema (mostly string types) to read the full file
            sampled_schema = sample_df.schema
            df = self.spark.read \
                .option("header", "true") \
                .schema(sampled_schema) \
                .csv(file_path)

            # Do not cache globally; analysis will sample via head/limit
            return df
        except Exception:
            logger.exception(f"Error reading CSV {file_path}")
            return None

    def _read_excel_file(self, file_path: str, file_name: str):
        """Read Excel file using Azure Blob Storage client and pandas"""
        try:
            logger.debug(f"Attempting to read Excel file: {file_name}")
            
            # Extract blob info from abfss path
            # Format: abfss://container@account.dfs.core.windows.net/path
            import re
            match = re.match(r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)', file_path)
            if not match:
                logger.error(f"Invalid abfss path format: {file_path}")
                return None
            
            container_name, account_name, blob_path = match.groups()
            
            # Get connection string from config
            connection_string =  config["connection_string"]
            if not connection_string:
                logger.error("Azure connection string not found in config")
                return None
            
            # Connect to blob storage
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            
            # Download file to memory
            stream = io.BytesIO()
            stream.write(blob_client.download_blob().readall())
            stream.seek(0)
            
            # Read with pandas
            pandas_df = pd.read_excel(stream, sheet_name=0)  # Read first sheet
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(pandas_df)
            
            logger.debug(f"Successfully read Excel file: {file_name}")
            return spark_df
                
        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {e}")
            return None
    
    # -------------------------
    # Schema fingerprint
    # -------------------------
    def _normalize_data_type(self, data_type: str) -> str:
        """Normalize data types for better grouping"""
        data_type_lower = str(data_type).lower()
        if 'string' in data_type_lower or 'varchar' in data_type_lower or 'char' in data_type_lower:
            return 'string'
        if 'int' in data_type_lower or 'long' in data_type_lower or 'bigint' in data_type_lower:
            return 'integer'
        if 'double' in data_type_lower or 'float' in data_type_lower or 'decimal' in data_type_lower:
            return 'numeric'
        if 'bool' in data_type_lower or 'boolean' in data_type_lower:
            return 'boolean'
        if 'date' in data_type_lower and 'timestamp' not in data_type_lower:
            return 'date'
        if 'timestamp' in data_type_lower:
            return 'timestamp'
        return 'other'

    def _calculate_schema_fingerprint(self, column_names: List[str], column_types: Dict[str, str]) -> str:
        """Calculate a safe schema fingerprint (order-independent)"""
        normalized_pairs = []
        for orig_col in column_names:
            normalized = re.sub(r'[^a-zA-Z0-9]', '_', orig_col.lower().strip())
            dtype = column_types.get(orig_col, 'unknown')
            normalized_type = self._normalize_data_type(str(dtype))
            normalized_pairs.append(f"{normalized}:{normalized_type}")
        normalized_pairs.sort()
        signature = "|".join(normalized_pairs)
        return hashlib.md5(signature.encode("utf-8")).hexdigest()

    # -------------------------
    # Data pattern analysis
    # -------------------------
    def _analyze_data_patterns(self, sample_data: List[Dict], column_types: Dict[str, str]) -> Dict[str, Any]:
        """Analyze data patterns in the sample"""
        if not sample_data:
            return {}

        patterns = {}
        for col_name in column_types.keys():
            col_values = [row.get(col_name) for row in sample_data if row.get(col_name) is not None]
            if not col_values:
                continue

            col_analysis = {
                'non_null_count': len(col_values),
                'sample_values': col_values[:3],
                'unique_count': len(set(str(v) for v in col_values)),
                'data_type': column_types[col_name]
            }

            ntype = self._normalize_data_type(column_types[col_name])
            if ntype == 'string':
                lengths = [len(str(v)) for v in col_values]
                col_analysis['avg_length'] = sum(lengths) / len(lengths)
                col_analysis['max_length'] = max(lengths)
            elif ntype in ['integer', 'numeric']:
                try:
                    numeric_values = [float(v) for v in col_values if v is not None]
                    if numeric_values:
                        col_analysis['min_value'] = min(numeric_values)
                        col_analysis['max_value'] = max(numeric_values)
                        col_analysis['avg_value'] = sum(numeric_values) / len(numeric_values)
                except Exception:
                    pass

            patterns[col_name] = col_analysis

        return patterns

    # -------------------------
    # Analyze single file
    # -------------------------
    def analyze_file(self, file_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze a single file (CSV or Excel) and extract metadata"""
        file_path = file_info['path']
        file_name = file_info['name']
        file_ext = file_info['extension']

        try:
            logger.debug(f"Analyzing file: {file_name}")

            # Read file based on type
            if file_ext == '.csv':
                df = self._read_csv_file(file_path)
            elif file_ext in ['.xlsx', '.xls'] and EXCEL_SUPPORT_AVAILABLE:
                df = self._read_excel_file(file_path, file_name)
            elif file_ext in ['.xlsx', '.xls']:
                # Try fallback despite EXCEL_SUPPORT_AVAILABLE flag (spark-excel may work)
                df = self._read_excel_file(file_path, file_name)
            else:
                logger.warning(f"Unsupported file type: {file_ext}")
                return None

            if df is None:
                logger.warning(f"No dataframe returned for {file_name}")
                return None

            # Schema and columns
            schema = df.schema
            column_names = df.columns
            column_count = len(column_names)
            column_types = {field.name: str(field.dataType) for field in schema.fields}

            # Row count estimate using a small limit (avoid full count)
            row_count = "unknown"
            try:
                small_count = df.limit(1001).count()
                row_count = small_count if small_count < 1001 else "1000+ (large)"
            except Exception:
                logger.debug("Row count estimation failed; leaving as unknown")

            # Sample data
            sample_data = []
            try:
                rows = df.head(MAX_SAMPLE_ROWS)
                # head() may return a single Row or list
                if isinstance(rows, list):
                    for r in rows:
                        sample_data.append(r.asDict())
                else:
                    sample_data.append(rows.asDict())
            except Exception:
                logger.debug("Sample extraction failed")

            # Schema fingerprint and data patterns
            schema_fingerprint = self._calculate_schema_fingerprint(column_names, column_types)
            data_patterns = self._analyze_data_patterns(sample_data, column_types)

            analysis_result = {
                'file_info': file_info,
                'schema': {
                    'column_names': column_names,
                    'column_types': column_types,
                    'column_count': column_count,
                    'schema_fingerprint': schema_fingerprint
                },
                'data_info': {
                    'row_count': row_count,
                    'sample_data': sample_data,
                    'data_patterns': data_patterns
                },
                'analysis_timestamp': datetime.datetime.now().isoformat()
            }

            logger.debug(f"Analyzed {file_name}: {column_count} cols")
            return analysis_result
        except Exception:
            logger.exception(f"Error analyzing file {file_name}")
            return None

    # -------------------------
    # Schema similarity & grouping
    # -------------------------
    def calculate_schema_similarity(self, schema1: Dict, schema2: Dict) -> float:
        """Calculate similarity between two schemas"""
        cols1 = set(schema1['column_names'])
        cols2 = set(schema2['column_names'])

        common_columns = cols1.intersection(cols2)
        total_columns = cols1.union(cols2)
        if not total_columns:
            return 0.0

        name_similarity = len(common_columns) / len(total_columns)

        type_compatibility = 0.0
        if common_columns:
            compatible_types = 0
            for col in common_columns:
                t1 = self._normalize_data_type(schema1['column_types'].get(col, 'unknown'))
                t2 = self._normalize_data_type(schema2['column_types'].get(col, 'unknown'))
                if t1 == t2:
                    compatible_types += 1
            type_compatibility = compatible_types / len(common_columns)

        similarity = (name_similarity * 0.7) + (type_compatibility * 0.3)
        return similarity

    def suggest_parquet_groupings(self, analysis_results: List[Dict]) -> Dict[str, Any]:
        """Suggest how files should be grouped for Parquet conversion"""
        if not analysis_results:
            return {}

        # Group by exact fingerprint
        exact_groups = defaultdict(list)
        for result in analysis_results:
            if result and 'schema' in result:
                exact_groups[result['schema']['schema_fingerprint']].append(result)

        suggested_groups = {}
        group_counter = 1

        # Exact match groups
        for fingerprint, files in exact_groups.items():
            if len(files) > 1:
                group_name = f"parquet_group_{group_counter:03d}_exact_match"
                suggested_groups[group_name] = {
                    'files': files,
                    'grouping_reason': 'Identical schema',
                    'schema_similarity': 1.0,
                    'recommended_parquet_name': f"{group_name}.parquet",
                    'total_size_mb': sum(f['file_info'].get('size_mb', 0.0) for f in files),
                    'total_files': len(files),
                    'sample_schema': files[0]['schema']
                }
                group_counter += 1

        # Collect singletons for similarity grouping
        ungrouped = []
        for fingerprint, files in exact_groups.items():
            if len(files) == 1:
                ungrouped.extend(files)

        while ungrouped:
            base = ungrouped.pop(0)
            similar = [base]
            remaining = []
            for other in ungrouped:
                sim = self.calculate_schema_similarity(base['schema'], other['schema'])
                if sim >= SIMILARITY_THRESHOLD:
                    similar.append(other)
                else:
                    remaining.append(other)
            ungrouped = remaining

            if len(similar) > 1:
                group_name = f"parquet_group_{group_counter:03d}_similar_schema"
                suggested_groups[group_name] = {
                    'files': similar,
                    'grouping_reason': f"Similar schema (>= {SIMILARITY_THRESHOLD})",
                    'schema_similarity': min(self.calculate_schema_similarity(base['schema'], f['schema']) for f in similar[1:]) if len(similar) > 1 else 1.0,
                    'recommended_parquet_name': f"{group_name}.parquet",
                    'total_size_mb': sum(f['file_info'].get('size_mb', 0.0) for f in similar),
                    'total_files': len(similar),
                    'sample_schema': base['schema']
                }
                group_counter += 1
            else:
                group_name = f"individual_file_{group_counter:03d}"
                suggested_groups[group_name] = {
                    'files': similar,
                    'grouping_reason': 'Unique schema - convert individually',
                    'schema_similarity': 1.0,
                    'recommended_parquet_name': f"{Path(base['file_info']['name']).stem}.parquet",
                    'total_size_mb': base['file_info'].get('size_mb', 0.0),
                    'total_files': 1,
                    'sample_schema': base['schema']
                }
                group_counter += 1

        return suggested_groups

    # -------------------------
    # Reporting & saving
    # -------------------------
    def generate_analysis_report(self, groupings: Dict[str, Any], input_dir: str = "") -> str:
        """Generate a simple textual report"""
        lines = []
        lines.append("=" * 80)
        lines.append("         AZURE BLOB STORAGE FILE ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append(f"Analysis Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Input Directory: {input_dir}")
        lines.append("")

        total_files = sum(group['total_files'] for group in groupings.values()) if groupings else 0
        total_size = sum(group['total_size_mb'] for group in groupings.values()) if groupings else 0.0
        total_groups = len(groupings)

        lines.append("📊 SUMMARY STATISTICS:")
        lines.append(f"   • Total files analyzed: {total_files}")
        lines.append(f"   • Total size (MB): {total_size:.2f}")
        lines.append(f"   • Suggested Parquet groups: {total_groups}")
        lines.append("")

        lines.append("📁 SUGGESTED PARQUET GROUPINGS:")
        lines.append("")
        for gname, ginfo in groupings.items():
            lines.append(f"Group: {gname}")
            lines.append(f"   Reason: {ginfo['grouping_reason']}")
            lines.append(f"   Recommended Parquet: {ginfo['recommended_parquet_name']}")
            lines.append(f"   Files: {ginfo['total_files']}")
            lines.append(f"   Total Size: {ginfo['total_size_mb']:.2f} MB")
            lines.append(f"   Schema Similarity: {ginfo['schema_similarity']:.2f}")
            sample_schema = ginfo.get('sample_schema', {})
            lines.append(f"   Columns ({sample_schema.get('column_count', 0)}):")
            for col, ctype in sample_schema.get('column_types', {}).items():
                lines.append(f"      - {col}: {ctype}")
            lines.append("   Files in group:")
            for f in ginfo['files']:
                fname = f['file_info']['name']
                fsize = f['file_info'].get('size_mb', 0.0)
                rows = f['data_info'].get('row_count', 'unknown')
                lines.append(f"      - {fname} ({fsize:.2f} MB, {rows} rows)")
            lines.append("")
        return "\n".join(lines)

    def save_analysis_results(self, groupings: Dict, detailed_results: List[Dict], input_dir: str = ""):
        """Save analysis results to local filesystem and attempt to write to output_dir using dbutils"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_content = self.generate_analysis_report(groupings, input_dir)
        detailed_data = {
            'analysis_metadata': {
                'timestamp': timestamp,
                'input_directory': input_dir,
                'configuration': {
                    'max_sample_rows': MAX_SAMPLE_ROWS,
                    'similarity_threshold': SIMILARITY_THRESHOLD,
                    'supported_extensions': SUPPORTED_EXTENSIONS + (EXCEL_EXTENSIONS if EXCEL_SUPPORT_AVAILABLE else [])
                }
            },
            'grouping_suggestions': groupings,
            'detailed_file_analysis': detailed_results
        }
        json_content = json.dumps(detailed_data, indent=2, default=str)

        # local saves
        local_report_path = f"/tmp/parquet_grouping_analysis_{timestamp}.txt"
        local_json_path = f"/tmp/detailed_analysis_{timestamp}.json"
        with open(local_report_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        with open(local_json_path, "w", encoding="utf-8") as f:
            f.write(json_content)
        logger.info(f"Saved local analysis report: {local_report_path}")
        logger.info(f"Saved local JSON results: {local_json_path}")

        # Try to save to OUTPUT_DIR using dbutils.fs.put (suitable for small files)
        global OUTPUT_DIR
        if OUTPUT_DIR:
            try:
                out_report_path = f"{OUTPUT_DIR}/parquet_grouping_analysis_{timestamp}.txt"
                out_json_path = f"{OUTPUT_DIR}/detailed_analysis_{timestamp}.json"
                # dbutils.fs.put expects a path and a string; works for small files
                dbutils.fs.put(out_report_path, report_content, overwrite=True)
                dbutils.fs.put(out_json_path, json_content, overwrite=True)
                logger.info(f"Saved report to: {out_report_path}")
                logger.info(f"Saved json to: {out_json_path}")
                return out_report_path, out_json_path
            except Exception:
                logger.exception("Saving to OUTPUT_DIR via dbutils.fs.put failed; returning local paths")

        return local_report_path, local_json_path

# =============================================================================
# Main
# =============================================================================
def main():
    """
    Main execution function optimized for single-user Spark cluster.

    Configuration priority:
      1) Environment variables: STORAGE_ACCOUNT, CONTAINER, BASE_PATH, OUTPUT_PATH
      2) dbutils.secrets (placeholder keys) - adjust scope/key names for your workspace
      3) Defaults (for quick dev only)
    """
    # Load configuration (prefer env vars). Adjust secret scope/key as needed.
    try:
        STORAGE_ACCOUNT =config["storage_account"]
        if not STORAGE_ACCOUNT:
            try:
                # replace '<scope>' and '<key>' with your secret scope/name
                STORAGE_ACCOUNT = dbutils.secrets.get(scope="<scope>", key="<storage-account-name>")
            except Exception:
                # fallback default (must be overridden)
                STORAGE_ACCOUNT = os.environ.get("DEFAULT_STORAGE_ACCOUNT", "")
        CONTAINER = os.environ.get("CONTAINER", "raw-data")
        BASE_PATH = os.environ.get("BASE_PATH", "6.SGD/RepRisk/30-April-2025")
        OUTPUT_PATH = os.environ.get("OUTPUT_PATH", BASE_PATH + "/analysis")
    except Exception:
        logger.exception("Error loading configuration; please set environment variables or dbutils secrets")
        raise

    input_dir = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{BASE_PATH}"
    output_dir = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{OUTPUT_PATH}"

    # Set module-level OUTPUT_DIR for save function
    global OUTPUT_DIR
    OUTPUT_DIR = output_dir

    spark = get_or_create_spark_session()
    analyzer = FileAnalyzer(spark)

    try:
        logger.info("=" * 60)
        logger.info("AZURE BLOB FILE ANALYZER - SINGLE USER CLUSTER")
        logger.info("=" * 60)
        logger.info(f"Input Directory: {input_dir}")
        logger.info(f"Output Directory: {output_dir}")
        logger.info(f"Spark Version: {spark.version}")
        logger.info("=" * 60)

        # Discover
        logger.info("🔍 Discovering files in Azure Blob Storage...")
        discovered_files = analyzer.discover_files(input_dir)

        if not discovered_files:
            logger.warning("No supported files found in the specified directory.")
            return

        logger.info(f"Found {len(discovered_files)} files to analyze")

        # Analyze
        logger.info("📊 Analyzing file structures and content...")
        analysis_results = []
        for fi in discovered_files:
            logger.info(f"Analyzing: {fi['name']} ({fi['extension']})")
            res = analyzer.analyze_file(fi)
            if res:
                analysis_results.append(res)
                logger.info(f"✅ Analyzed: {fi['name']}")
            else:
                logger.warning(f"⚠️ Could not analyze: {fi['name']}")

        if not analysis_results:
            logger.error("❌ No files were successfully analyzed.")
            return

        # Suggest groupings
        logger.info("🎯 Generating Parquet grouping suggestions...")
        groupings = analyzer.suggest_parquet_groupings(analysis_results)

        # Display concise results in logs
        logger.info("\n" + "=" * 60)
        logger.info("PARQUET GROUPING SUGGESTIONS:")
        logger.info("=" * 60)
        for gname, ginfo in groupings.items():
            logger.info(f"📁 {gname} - files: {ginfo['total_files']} size_mb: {ginfo['total_size_mb']:.2f} reason: {ginfo['grouping_reason']}")

        # Save results
        logger.info("💾 Saving analysis results...")
        try:
            report_path, json_path = analyzer.save_analysis_results(groupings, analysis_results, input_dir)
            logger.info(f"📄 Report: {report_path}")
            logger.info(f"📋 JSON:   {json_path}")
        except Exception:
            logger.exception("Could not save results; they are available locally in /tmp")

        logger.info("\n" + "=" * 60)
        logger.info("✅ ANALYSIS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        return {
            'groupings': groupings,
            'analysis_results': analysis_results,
            'input_dir': input_dir,
            'output_dir': output_dir
        }

    except Exception:
        logger.exception("❌ Error in main execution")
        raise
    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            logger.debug("Failed to clear Spark cache")
        gc.collect()

# ENTRYPOINT
if __name__ == "__main__":
    main()
