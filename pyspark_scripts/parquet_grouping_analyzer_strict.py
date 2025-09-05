#!/usr/bin/env python3
"""
Parquet Grouping Analyzer for Azure Blob Storage

This script analyzes CSV and Excel files in Azure Blob Storage,
examines their structure and content, and suggests optimal groupings for 
Parquet file conversion based on schema similarity and data characteristics.

FEATURES:
- Reads from Azure Blob Storage using abfss:// protocol
- Analyzes top 5 records of each CSV/Excel file
- Suggests file groupings based on schema compatibility
- Works with standard Spark (no Databricks dependencies)
- Supports mixed CSV and Excel file analysis

Requirements:
    - pandas
    - pyspark
    - openpyxl (for Excel support)
    - xlrd (for legacy Excel support)
    - azure-storage-blob (for Azure Blob Storage access)

Azure Blob Storage Configuration:
    Create file_analyzer_config.py with:
    config = {
        "storage_account": "your_storage_account_name",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
    }
"""

import os
import re
import pandas as pd
import traceback
from pathlib import Path
import time
import logging
import datetime
import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
import tempfile
import shutil
import uuid
import io
from azure.storage.blob import BlobServiceClient
EXCEL_SUPPORT_AVAILABLE = True

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# Analysis Configuration
MAX_SAMPLE_ROWS = 5                           # Number of top records to analyze per file
SUPPORTED_EXTENSIONS = [".csv"]               # CSV file extensions
EXCEL_EXTENSIONS = [".xlsx", ".xls"]          # Excel file extensions
SIMILARITY_THRESHOLD = 0.8                    # Schema similarity threshold for grouping (0.0-1.0)

# =============================================================================
# END CONFIGURATION SECTION
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_or_create_spark_session():
    """Get or create Spark session with proper configuration"""
    try:
        # Try to get existing session first
        spark = SparkSession.getActiveSession()
        if spark:
            print("✓ Using existing Spark session")
            return spark
        
        # Create new session if none exists
        spark = SparkSession.builder \
            .appName("File_Analyzer_Parquet_Grouping") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        print("✓ Created new Spark session")
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        raise

# Note: Spark session will be created in main() function for single-user cluster optimization


class FileAnalyzer:
    """Class to analyze files and suggest Parquet groupings"""
    
    def __init__(self, spark_session, config=None):
        self.spark = spark_session
        self.config = config or {}
        self.file_metadata = {}
        self.schema_groups = defaultdict(list)
        self.analysis_results = {}
    
    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        try:
            # Use Spark to create directory - this works with any Spark setup
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS temp_file_analyzer")
            logger.info(f"Ensured output directory setup completed")
        except Exception as e:
            logger.warning(f"Could not create temp database (this is normal): {e}")
            # This is not critical for file analysis
    
    def discover_files(self, directory_path: str) -> List[Dict[str, Any]]:
        """Discover individual files using Databricks dbutils"""
        discovered_files = []
        
        try:
            # Use dbutils to list individual files (this works on Databricks)
            try:
                file_list = dbutils.fs.ls(directory_path)
                
                all_supported = SUPPORTED_EXTENSIONS + (EXCEL_EXTENSIONS if EXCEL_SUPPORT_AVAILABLE else [])
                
                for file_info in file_list:
                    if not file_info.isDir():
                        file_path = file_info.path
                        file_name = file_info.name
                        file_size = file_info.size
                        
                        # Check if file has supported extension
                        file_ext = os.path.splitext(file_name.lower())[1]
                        if file_ext in all_supported:
                            discovered_files.append({
                                'path': file_path,
                                'name': file_name,
                                'size': file_size,
                                'extension': file_ext,
                                'size_mb': round(file_size / (1024 * 1024), 2)
                            })
                            logger.debug(f"Found file: {file_name} ({file_ext})")
                        else:
                            logger.debug(f"Skipping unsupported file: {file_name}")
                    else:
                        logger.debug(f"Skipping directory: {file_info.name}")
                
                logger.info(f"Discovered {len(discovered_files)} individual files")
                return discovered_files
                
            except Exception as dbutils_error:
                logger.warning(f"dbutils.fs.ls failed: {dbutils_error}")
                # Fallback to pattern-based discovery
                logger.info("Falling back to pattern-based file discovery")
                
                all_supported = SUPPORTED_EXTENSIONS + (EXCEL_EXTENSIONS if EXCEL_SUPPORT_AVAILABLE else [])
                
                for ext in all_supported:
                    try:
                        pattern = f"{directory_path}/*{ext}"
                        logger.debug(f"Trying pattern: {pattern}")
                        
                        if ext == '.csv':
                            # Test if CSV files exist
                            test_df = self.spark.read.option("header", "true").csv(pattern).limit(1)
                            test_df.count()
                            
                            # Add pattern as single entry (not ideal but functional)
                            discovered_files.append({
                                'path': pattern,
                                'name': f"all_csv_files",
                                'size': 0,
                                'extension': ext,
                                'size_mb': 0.0
                            })
                            logger.debug(f"Found CSV files with pattern: {pattern}")
                            
                    except Exception as pattern_error:
                        logger.debug(f"No files found for pattern {pattern}: {pattern_error}")
                        continue
                
                if not discovered_files:
                    raise Exception(f"Could not discover any files in {directory_path}")
                
                logger.info(f"Discovered {len(discovered_files)} file patterns")
                return discovered_files
            
        except Exception as e:
            logger.error(f"Error discovering files in {directory_path}: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def analyze_file(self, file_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze a single file (CSV or Excel) and extract metadata"""
        file_path = file_info['path']
        file_name = file_info['name']
        file_ext = file_info['extension']
        
        try:
            logger.debug(f"Analyzing file: {file_name}")
            
            # Handle different file types
            if file_ext == '.csv':
                df = self._read_csv_file(file_path)
            elif file_ext in ['.xlsx', '.xls'] and EXCEL_SUPPORT_AVAILABLE:
                df = self._read_excel_file(file_path, file_name)
            else:
                logger.warning(f"Unsupported file type: {file_ext}")
                return None
            
            if df is None:
                return None
                
            # Get schema information
            schema = df.schema
            column_names = df.columns
            column_count = len(column_names)
            
            # Get data types
            column_types = {field.name: str(field.dataType) for field in schema.fields}
            
            # Get row count (limit to avoid long operations on large files)
            try:
                # For analysis purposes, we don't need exact count for very large files
                sample_for_count = df.limit(10000)  # Limit to 10K rows for counting
                row_count = sample_for_count.count()
                if row_count == 10000:
                    # If we hit the limit, estimate based on file pattern
                    row_count = f"10,000+ (estimated large dataset)"
            except Exception as e:
                logger.warning(f"Could not get row count for {file_name}: {e}")
                row_count = -1
            
            # Get sample data (top 5 records)
            sample_data = []
            try:
                sample_rows = df.limit(MAX_SAMPLE_ROWS).collect()
                for row in sample_rows:
                    sample_data.append(row.asDict())
            except Exception as e:
                logger.warning(f"Could not get sample data for {file_name}: {e}")
            
            # Calculate schema fingerprint for grouping
            schema_fingerprint = self._calculate_schema_fingerprint(column_names, column_types)
            
            # Analyze data patterns
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
            
            logger.debug(f"Successfully analyzed {file_name}: {column_count} columns, {row_count} rows")
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error analyzing file {file_name}: {e}")
            logger.debug(traceback.format_exc())
            return None
    
    def _read_csv_file(self, file_path: str):
        """Read CSV file using Spark with optimizations for large datasets"""
        try:
            # Add optimizations for better performance on large files
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "false") \
                .option("escape", "\"") \
                .csv(file_path)
            
            # Cache a small sample for analysis to avoid repeated reads
            sample_df = df.limit(1000).cache()  # Cache first 1000 rows
            logger.debug(f"Successfully loaded CSV file: {file_path}")
            return sample_df
            
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
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
    
    def _calculate_schema_fingerprint(self, column_names: List[str], column_types: Dict[str, str]) -> str:
        """Calculate a fingerprint for the schema to enable grouping"""
        # Normalize column names (lowercase, remove spaces/special chars)
        normalized_columns = []
        for col in column_names:
            normalized = re.sub(r'[^a-zA-Z0-9]', '_', col.lower().strip())
            normalized_columns.append(normalized)
        
        # Create a signature combining column names and types
        schema_signature = []
        for col in normalized_columns:
            original_col = column_names[normalized_columns.index(col)]
            col_type = column_types.get(original_col, 'unknown')
            # Normalize data types (group similar types)
            normalized_type = self._normalize_data_type(col_type)
            schema_signature.append(f"{col}:{normalized_type}")
        
        # Sort to make order-independent
        schema_signature.sort()
        
        # Create hash
        signature_string = "|".join(schema_signature)
        fingerprint = hashlib.md5(signature_string.encode()).hexdigest()
        
        return fingerprint
    
    def _normalize_data_type(self, data_type: str) -> str:
        """Normalize data types for better grouping"""
        data_type_lower = data_type.lower()
        
        if 'string' in data_type_lower or 'varchar' in data_type_lower:
            return 'string'
        elif 'int' in data_type_lower or 'long' in data_type_lower or 'bigint' in data_type_lower:
            return 'integer'
        elif 'double' in data_type_lower or 'float' in data_type_lower or 'decimal' in data_type_lower:
            return 'numeric'
        elif 'bool' in data_type_lower:
            return 'boolean'
        elif 'date' in data_type_lower:
            return 'date'
        elif 'timestamp' in data_type_lower:
            return 'timestamp'
        else:
            return 'other'
    
    def _analyze_data_patterns(self, sample_data: List[Dict], column_types: Dict[str, str]) -> Dict[str, Any]:
        """Analyze data patterns in the sample"""
        if not sample_data:
            return {}
        
        patterns = {}
        
        # Analyze each column
        for col_name in column_types.keys():
            col_values = [row.get(col_name) for row in sample_data if row.get(col_name) is not None]
            
            if not col_values:
                continue
            
            col_analysis = {
                'non_null_count': len(col_values),
                'sample_values': col_values[:3],  # First 3 non-null values
                'unique_count': len(set(str(v) for v in col_values)),
                'data_type': column_types[col_name]
            }
            
            # Add type-specific analysis
            if self._normalize_data_type(column_types[col_name]) == 'string':
                col_analysis['avg_length'] = sum(len(str(v)) for v in col_values) / len(col_values)
                col_analysis['max_length'] = max(len(str(v)) for v in col_values)
            elif self._normalize_data_type(column_types[col_name]) in ['integer', 'numeric']:
                try:
                    numeric_values = [float(v) for v in col_values if v is not None]
                    if numeric_values:
                        col_analysis['min_value'] = min(numeric_values)
                        col_analysis['max_value'] = max(numeric_values)
                        col_analysis['avg_value'] = sum(numeric_values) / len(numeric_values)
                except:
                    pass
            
            patterns[col_name] = col_analysis
        
        return patterns
    
    def calculate_schema_similarity(self, schema1: Dict, schema2: Dict) -> float:
        """Calculate similarity between two schemas"""
        cols1 = set(schema1['column_names'])
        cols2 = set(schema2['column_names'])
        
        # Calculate column name overlap
        common_columns = cols1.intersection(cols2)
        total_columns = cols1.union(cols2)
        
        if not total_columns:
            return 0.0
        
        name_similarity = len(common_columns) / len(total_columns)
        
        # Calculate data type compatibility for common columns
        type_compatibility = 0.0
        if common_columns:
            compatible_types = 0
            for col in common_columns:
                type1 = self._normalize_data_type(schema1['column_types'][col])
                type2 = self._normalize_data_type(schema2['column_types'][col])
                if type1 == type2:
                    compatible_types += 1
            type_compatibility = compatible_types / len(common_columns)
        
        # Weighted similarity score
        similarity = (name_similarity * 0.7) + (type_compatibility * 0.3)
        return similarity
    
    def suggest_parquet_groupings(self, analysis_results: List[Dict]) -> Dict[str, List[Dict]]:
        """Suggest how files should be grouped for Parquet conversion"""
        if not analysis_results:
            return {}
        
        # Group files by exact schema fingerprint first
        exact_groups = defaultdict(list)
        for result in analysis_results:
            if result and 'schema' in result:
                fingerprint = result['schema']['schema_fingerprint']
                exact_groups[fingerprint].append(result)
        
        # For files that don't have exact matches, find similar schemas
        suggested_groups = {}
        group_counter = 1
        
        # Process exact match groups first
        for fingerprint, files in exact_groups.items():
            if len(files) > 1:  # Only group if multiple files
                group_name = f"parquet_group_{group_counter:03d}_exact_match"
                suggested_groups[group_name] = {
                    'files': files,
                    'grouping_reason': 'Identical schema',
                    'schema_similarity': 1.0,
                    'recommended_parquet_name': f"{group_name}.parquet",
                    'total_size_mb': sum(f['file_info']['size_mb'] for f in files),
                    'total_files': len(files),
                    'sample_schema': files[0]['schema']
                }
                group_counter += 1
        
        # Process remaining single files and look for similar schemas
        ungrouped_files = []
        for fingerprint, files in exact_groups.items():
            if len(files) == 1:
                ungrouped_files.extend(files)
        
        # Try to group similar schemas
        while ungrouped_files:
            base_file = ungrouped_files.pop(0)
            similar_files = [base_file]
            
            # Find files with similar schemas
            remaining_files = []
            for other_file in ungrouped_files:
                similarity = self.calculate_schema_similarity(
                    base_file['schema'], 
                    other_file['schema']
                )
                
                if similarity >= SIMILARITY_THRESHOLD:
                    similar_files.append(other_file)
                else:
                    remaining_files.append(other_file)
            
            ungrouped_files = remaining_files
            
            # Create group if we have multiple similar files
            if len(similar_files) > 1:
                group_name = f"parquet_group_{group_counter:03d}_similar_schema"
                suggested_groups[group_name] = {
                    'files': similar_files,
                    'grouping_reason': f'Similar schema (similarity >= {SIMILARITY_THRESHOLD})',
                    'schema_similarity': min(
                        self.calculate_schema_similarity(base_file['schema'], f['schema']) 
                        for f in similar_files[1:]
                    ),
                    'recommended_parquet_name': f"{group_name}.parquet",
                    'total_size_mb': sum(f['file_info']['size_mb'] for f in similar_files),
                    'total_files': len(similar_files),
                    'sample_schema': base_file['schema']
                }
                group_counter += 1
            else:
                # Single file - suggest individual conversion
                group_name = f"individual_file_{group_counter:03d}"
                suggested_groups[group_name] = {
                    'files': similar_files,
                    'grouping_reason': 'Unique schema - convert individually',
                    'schema_similarity': 1.0,
                    'recommended_parquet_name': f"{os.path.splitext(base_file['file_info']['name'])[0]}.parquet",
                    'total_size_mb': base_file['file_info']['size_mb'],
                    'total_files': 1,
                    'sample_schema': base_file['schema']
                }
                group_counter += 1
        
        return suggested_groups
    
    def generate_analysis_report(self, groupings: Dict[str, List[Dict]], input_dir: str = "") -> str:
        """Generate a comprehensive analysis report"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("         AZURE BLOB STORAGE FILE ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Analysis Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Input Directory: {input_dir}")
        report_lines.append("")
        
        # Summary statistics
        total_files = sum(group['total_files'] for group in groupings.values())
        total_size = sum(group['total_size_mb'] for group in groupings.values())
        total_groups = len(groupings)
        
        report_lines.append("📊 SUMMARY STATISTICS:")
        report_lines.append(f"   • Total files analyzed: {total_files}")
        report_lines.append(f"   • Total size: {total_size:.2f} MB")
        report_lines.append(f"   • Suggested Parquet groups: {total_groups}")
        report_lines.append("")
        
        # Grouping details
        report_lines.append("📁 SUGGESTED PARQUET GROUPINGS:")
        report_lines.append("")
        
        for group_name, group_info in groupings.items():
            report_lines.append(f"Group: {group_name}")
            report_lines.append(f"   Reason: {group_info['grouping_reason']}")
            report_lines.append(f"   Recommended Parquet: {group_info['recommended_parquet_name']}")
            report_lines.append(f"   Files: {group_info['total_files']}")
            report_lines.append(f"   Total Size: {group_info['total_size_mb']:.2f} MB")
            report_lines.append(f"   Schema Similarity: {group_info['schema_similarity']:.2f}")
            
            # Schema details
            sample_schema = group_info['sample_schema']
            report_lines.append(f"   Columns ({sample_schema['column_count']}):")
            for col_name, col_type in sample_schema['column_types'].items():
                report_lines.append(f"      - {col_name}: {col_type}")
            
            # File list
            report_lines.append("   Files in group:")
            for file_info in group_info['files']:
                file_name = file_info['file_info']['name']
                file_size = file_info['file_info']['size_mb']
                row_count = file_info['data_info']['row_count']
                report_lines.append(f"      - {file_name} ({file_size:.2f} MB, {row_count} rows)")
            
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    def save_analysis_results(self, groupings: Dict, detailed_results: List[Dict], input_dir: str = "", output_dir: str = ""):
        """Save analysis results to local filesystem and optionally to Azure Blob Storage"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Generate report content
            report_content = self.generate_analysis_report(groupings, input_dir)
            
            # Prepare detailed JSON data
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
            
            # Save to local filesystem first
            local_report_path = f"/tmp/parquet_grouping_analysis_{timestamp}.txt"
            local_json_path = f"/tmp/detailed_analysis_{timestamp}.json"
            
            with open(local_report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            with open(local_json_path, 'w', encoding='utf-8') as f:
                f.write(json_content)
            
            logger.info(f"Saved local analysis report: {local_report_path}")
            logger.info(f"Saved local JSON results: {local_json_path}")
            
            # Try to save to Azure Blob Storage as proper text files
            try:
                if output_dir:
                    # Save report as text file using dbutils
                    azure_report_path = f"{output_dir}/parquet_grouping_analysis_{timestamp}.txt"
                    dbutils.fs.put(azure_report_path, report_content, True)
                    logger.info(f"Saved Azure text report to: {azure_report_path}")
                    
                    # Save JSON as text file using dbutils
                    azure_json_path = f"{output_dir}/detailed_analysis_{timestamp}.json"
                    dbutils.fs.put(azure_json_path, json_content, True)
                    logger.info(f"Saved Azure JSON to: {azure_json_path}")
                    
                    return azure_report_path, azure_json_path
                else:
                    logger.info("No output directory provided, saving locally only")
                    return local_report_path, local_json_path
                
            except Exception as azure_error:
                logger.warning(f"Could not save to Azure Blob Storage: {azure_error}")
                logger.info("Results are available in local filesystem only")
                return local_report_path, local_json_path
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {e}")
            raise


def main():
    """Main execution function optimized for single-user Spark cluster"""
    
    # Configuration - Update these for your environment
    STORAGE_ACCOUNT = config["storage_account"]  # Replace with your storage account
    CONTAINER = "raw-data"              # Replace with your container
    BASE_PATH = "1.Corporate_Registry/SNP/30-Jun-2025"              # Replace with your data path
    OUTPUT_PATH = "1.Corporate_Registry/SNP/30-Jun-2025/analysis"  # Replace with output path
    
    # Build paths
    input_dir = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{BASE_PATH}"
    output_dir = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{OUTPUT_PATH}"
    
    # Get Spark session
    spark_session = get_or_create_spark_session()
    
    # Initialize analyzer with spark session and config
    analyzer = FileAnalyzer(spark_session, config)
    
    try:
        logger.info("=" * 60)
        logger.info("AZURE BLOB FILE ANALYZER - SINGLE USER CLUSTER")
        logger.info("=" * 60)
        logger.info(f"Input Directory: {input_dir}")
        logger.info(f"Output Directory: {output_dir}")
        logger.info(f"Spark Version: {spark_session.version}")
        logger.info("=" * 60)
        
        # Setup directories
        analyzer.setup_directories()
        
        # Discover files
        logger.info("🔍 Discovering files in Azure Blob Storage...")
        discovered_files = analyzer.discover_files(input_dir)
        
        if not discovered_files:
            logger.warning("No supported files found in the specified directory.")
            logger.info("💡 Make sure your path contains CSV or Excel files")
            return
        
        logger.info(f"Found {len(discovered_files)} file patterns to analyze")
        
        # Analyze each file pattern
        logger.info("📊 Analyzing file structures and content...")
        analysis_results = []
        
        for file_info in discovered_files:
            logger.info(f"Analyzing: {file_info['name']} ({file_info['extension']})")
            result = analyzer.analyze_file(file_info)
            if result:
                analysis_results.append(result)
                logger.info(f"✅ Successfully analyzed {file_info['name']}")
            else:
                logger.warning(f"⚠️ Could not analyze {file_info['name']}")
        
        logger.info(f"Successfully analyzed {len(analysis_results)} file patterns")
        
        if not analysis_results:
            logger.error("❌ No files were successfully analyzed.")
            logger.info("💡 Check your file paths and formats")
            return
        
        # Suggest groupings
        logger.info("🎯 Generating Parquet grouping suggestions...")
        groupings = analyzer.suggest_parquet_groupings(analysis_results)
        
        # Display results
        logger.info("=" * 60)
        logger.info("PARQUET GROUPING SUGGESTIONS:")
        logger.info("=" * 60)
        
        for group_name, group_info in groupings.items():
            logger.info(f"\n📁 {group_name}:")
            logger.info(f"   Files: {group_info['total_files']}")
            logger.info(f"   Size: {group_info['total_size_mb']:.2f} MB")
            logger.info(f"   Reason: {group_info['grouping_reason']}")
            logger.info(f"   Recommended: {group_info['recommended_parquet_name']}")
        
        # Save results
        logger.info("\n💾 Saving analysis results...")
        try:
            report_path, json_path = analyzer.save_analysis_results(groupings, analysis_results, input_dir, output_dir)
            logger.info(f"📄 Text Report: {report_path}")
            logger.info(f"📋 JSON Details: {json_path}")
        except Exception as save_error:
            logger.warning(f"Could not save results to blob storage: {save_error}")
            logger.info("Results are displayed above and available in Spark session")
        
        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("✅ ANALYSIS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"🎯 Suggested {len(groupings)} Parquet groupings")
        logger.info(f"📊 Analyzed {len(analysis_results)} file patterns")
        logger.info("💡 Update the configuration at the top of main() for your environment")
        
        # Return results for interactive use
        return {
            'groupings': groupings,
            'analysis_results': analysis_results,
            'input_dir': input_dir,
            'output_dir': output_dir
        }
        
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        logger.debug(traceback.format_exc())
        raise
    
    finally:
        # Clean up any temporary resources
        try:
            spark_session.catalog.clearCache()
            logger.info("🧹 Cleared Spark cache")
        except Exception as cleanup_error:
            logger.debug(f"Cache cleanup warning: {cleanup_error}")


# COMMAND ----------

if __name__ == "__main__":
    main()
