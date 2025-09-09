#!/usr/bin/env python3
"""
S&P Global Climate Data Processor for Databricks

This script processes S&P Global Climate Excel files,
extracts data for climate sustainability metrics,
and generates unified CSV output files.

FEATURES:
- Reads from Azure Blob Storage
- Generates combined CSV file with unified data
- Creates separate CSV files for individual data types
- Parallel processing with error handling and retry logic
- Comprehensive logging and file tracking

Requirements:
    - pandas
    - xlrd (for reading .xls input files)
    - dbutils (provided by Databricks runtime)

Azure Blob Storage Configuration:
    Update INPUT_DIR and OUTPUT_DIR with your actual Azure storage account and container paths.
"""

import os
import re
import pandas as pd
import traceback
from pathlib import Path
# Excel reading libraries (still needed for input file processing)
import xlrd     # For .xls input files
from copy import copy
import time
import logging
import random
import datetime # Added for date handling
import hashlib  # For filename hashing
import concurrent.futures # For parallel processing
import threading          # For thread identification in logs
import functools          # For lru_cache
import io
from pyspark.sql import SparkSession  # For distributed processing
from pyspark.sql import functions as F
from azure.storage.blob import BlobServiceClient

# =============================================================================
# CONFIGURATION SECTION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
# =============================================================================

# Azure Storage Account Configuration
STORAGE_ACCOUNT_NAME = config["storage_account"]  # Replace with your storage account name
CONTAINER_NAME = "raw-data"              # Replace with your container name

# Path Configuration
BASE_PATH = "4.ESG/SNP/30-Jun-2025"           # Base path within container
INPUT_SUBPATH = ""                             # Subpath for input files (empty = base path)
OUTPUT_SUBPATH = "output"                      # Subpath for output files

# Processing Configuration
MAX_RETRIES = 3                                # Maximum number of retries for failed files
BATCH_SIZE = 500                               # Number of files to process in each batch
MAX_WORKERS = 16                               # Maximum number of parallel worker threads

# File Filter Configuration
FILE_KEYWORD = "sustainabilityandclimateoverview"    # Keyword that must be in filename (lowercase)
SUPPORTED_EXTENSIONS = [".xls", ".xlsx"]       # Supported Excel file extensions

# Data Processing Limits (to avoid command result size limits)
MAX_RECORDS_PER_FILE = 1000000  # Maximum records to process per file (increased from 50k)
ENABLE_DATA_SAMPLING = False   # Disable data sampling to keep all data
SAMPLE_SIZE = 10000          # Sample size for large datasets (not used when sampling disabled)

# =============================================================================
# END CONFIGURATION SECTION
# =============================================================================

# Configure logging - MINIMAL OUTPUT
logging.basicConfig(
    level=logging.WARNING,  # Only show warnings and errors
    format='%(levelname)s: %(message)s'  # Simplified format
)
logger = logging.getLogger(__name__)

# Build paths from configuration
INPUT_DIR = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BASE_PATH}"
if INPUT_SUBPATH:
    INPUT_DIR = f"{INPUT_DIR}/{INPUT_SUBPATH}"

OUTPUT_DIR = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BASE_PATH}/{OUTPUT_SUBPATH}"

ERROR_DIR = f"{OUTPUT_DIR}/errors"
LOG_DIR = f"{OUTPUT_DIR}/logs"

# CSV Output Configuration
CSV_OUTPUT_FILE = f"{OUTPUT_DIR}/unified_climate_esg_data.csv"

# Initialize Spark Session for distributed processing
spark = SparkSession.builder.appName("S&P_Climate_Processor").getOrCreate()

# Optimize Spark configuration for better parallelization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "400")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Always use Azure Blob Storage for temporary files (works on all cluster types)
TEMP_DIR = f"{OUTPUT_DIR}/temp"
TEMP_LOCATION = "azure"
CLUSTER_TYPE = "Universal (Azure Blob Temp)"

# Ensure temp directory exists in Azure
dbutils.fs.mkdirs(TEMP_DIR)

# Minimal startup logging
print("S&P Climate Processor: Starting...")
print(f"Processing {len(dbutils.fs.ls(INPUT_DIR))} files from {INPUT_DIR}")

# Verify Azure temp directory is accessible
try:
    test_file = f"{TEMP_DIR}/startup_test_{int(time.time())}.txt"
    dbutils.fs.put(test_file, "test", True)
    dbutils.fs.rm(test_file)
    print("✓ Azure temp directory accessible")
except Exception as e:
    print(f"✗ Azure temp directory error: {e}")

# =============================================================================
# EXCEL READING FUNCTIONS (FROM SP_BUSINESS.PY)
# =============================================================================

def get_safer_temp_path(original_path):
    """Get a safe temporary path for processing files"""
    filename = original_path.split("/")[-1]
    
    # Always hash filenames with non-Latin characters
    if any(ord(c) > 127 for c in filename) or len(filename) > 150:
        filename_hash = hashlib.md5(filename.encode()).hexdigest()
        extension = os.path.splitext(filename)[1]
        safe_filename = f"{filename_hash}{extension}"
    else:
        safe_filename = filename
        
    temp_path = f"{TEMP_DIR}/{threading.get_ident()}_{safe_filename}"
    logger.debug(f"Creating temp path: {temp_path} (location={TEMP_LOCATION})")
    return temp_path


def copy_to_temp_and_get_local_path(source_path):
    """Copy file to Azure temp location and return local workspace path for processing"""
    azure_temp_path = get_safer_temp_path(source_path)
    
    # Copy to Azure temp first
    dbutils.fs.cp(source_path, azure_temp_path)
    
    # Create a local workspace temp path for actual processing
    filename = azure_temp_path.split("/")[-1]
    local_workspace_path = f"/Workspace/tmp/{filename}"
    
    # Ensure local workspace temp directory exists
    os.makedirs("/Workspace/tmp", exist_ok=True)
    
    # Copy from Azure temp to local workspace for processing
    dbutils.fs.cp(azure_temp_path, f"file:{local_workspace_path}")
    
    return local_workspace_path, azure_temp_path


def cleanup_temp_files(local_path, azure_temp_path=None):
    """Clean up temporary files from both local workspace and Azure locations"""
    try:
        # Clean up local workspace file
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.debug(f"Cleaned up local workspace temp file: {local_path}")
    except Exception as e:
        logger.warning(f"Could not remove local workspace temp file {local_path}: {e}")
    
    try:
        # Clean up Azure temp file
        if azure_temp_path:
            dbutils.fs.rm(azure_temp_path)
            logger.debug(f"Cleaned up Azure temp file: {azure_temp_path}")
    except Exception as e:
        logger.warning(f"Could not remove Azure temp file {azure_temp_path}: {e}")


def _read_excel_file(file_path: str, file_name: str):
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
        connection_string = config["connection_string"]
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
        
        logger.debug(f"Successfully read Excel file: {file_name}")
        return pandas_df
                
    except Exception as e:
        logger.error(f"Error reading Excel file {file_path}: {e}")
        return None


# =============================================================================
# EXTRACTION LOGIC (UNCHANGED AS REQUESTED)
# =============================================================================

def extract_and_process_file(input_file):
    """
    Extracts data from an S&P Global Excel file and returns structured data
    
    Parameters:
    - input_file: Path to the input S&P Global Excel file
    
    Returns:
    - (success, file_path, data_df, error_msg) tuple
    """
    # Get a unique thread ID for temp file naming
    thread_id = threading.get_ident()
    
    try:
        # Read Excel file using the new method
        df = _read_excel_file(input_file, os.path.basename(input_file))
        
        if df is None: 
            return (False, input_file, f"Failed to read Excel file {os.path.basename(input_file)}")

        # Convert DataFrame to rows for processing
        rows = [df.columns.tolist()] + df.values.tolist()
        
        # Extract company information and other data
        company_info = extract_company_info(rows, input_file, thread_id)
        company_name = company_info.get('company_name', 'Unknown Company')
        company_key = company_info.get('company_key', '')
        
        # Extract the description text
        description = extract_description(rows, thread_id)
        
        # First, analyze sections and their types
        sections = identify_all_sections(rows, thread_id)
        
        # Extract data points
        data_entries = extract_data_entries(rows, sections, thread_id)
        
        # Create a new dataframe with the structured data
        df_result = create_structured_dataframe(company_name, company_key, description, data_entries)
        
        # Apply data sampling if enabled and dataset is large
        if ENABLE_DATA_SAMPLING and len(df_result) > MAX_RECORDS_PER_FILE:
            df_result = df_result.sample(n=min(SAMPLE_SIZE, len(df_result)), random_state=42)
        
        # Add metadata columns directly to the dataframe
        df_result['source_file'] = input_file
        df_result['processed_timestamp'] = datetime.datetime.now().isoformat()
        
        # Return success and the data
        return (True, input_file, df_result, None)
            
    except Exception as e:
        return (False, input_file, None, str(e))

def extract_company_info(rows, input_file=None, thread_id=None):
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    company_info = {'company_name': '', 'company_key': ''}
    found_in_content = extract_from_excel_content_patterns(rows, company_info, thread_id)
    if found_in_content and company_info['company_name'] and company_info['company_key']:
        return company_info
    if input_file:
        extract_from_filename_patterns(input_file, company_info, thread_id)
        if company_info['company_name'] and company_info['company_key']:
            return company_info
    if not company_info['company_name'] or not company_info['company_key']:
        general_pattern_extraction(rows, company_info, thread_id)
    if not company_info['company_name']:
        company_info['company_name'] = "Unknown Company"
    return company_info

def extract_from_excel_content_patterns(rows, company_info, thread_id=None):
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    found_any = False
    for i, row in enumerate(rows[:20]):
        for j, cell in enumerate(row):
            if not cell or not isinstance(cell, str) or len(str(cell).strip()) < 10: continue
            cell_text = str(cell).strip()
            if " | " in cell_text:
                company_part = cell_text.split(" | ")[0].strip()
                extract_from_company_part(company_part, company_info, thread_id)
                found_any = True
                continue
            if "(" in cell_text and ")" in cell_text and re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', cell_text):
                extract_from_company_part(cell_text, company_info, thread_id)
                found_any = True
                continue
            match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', cell_text)
            if match:
                extract_from_company_part(cell_text, company_info, thread_id)
                found_any = True
    return found_any

def extract_from_company_part(text, company_info, thread_id=None):
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    exchange_symbol_match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', text)
    if exchange_symbol_match:
        exchange_symbol = exchange_symbol_match.group(1)
        parts = text.split(exchange_symbol)
        potential_name = re.sub(r'[,\.;:]$', '', parts[0].strip()).strip()
        if not company_info['company_name'] and potential_name: company_info['company_name'] = potential_name
        paren_matches = list(re.finditer(r'\(([^)]+)\)', text))
        if len(paren_matches) > 0:
            id_block = paren_matches[-1].group(0)
            full_key = f"{exchange_symbol} {id_block}"
        else:
            full_key = exchange_symbol
        if not company_info['company_key']: company_info['company_key'] = full_key
    else:
        paren_matches = list(re.finditer(r'\(([^)]+)\)', text))
        if len(paren_matches) > 0:
            last_paren = paren_matches[-1]
            name_part = text[:last_paren.start()].strip()
            key_part = last_paren.group(0)
            if not company_info['company_name'] and name_part: company_info['company_name'] = name_part
            if not company_info['company_key']: company_info['company_key'] = key_part

def extract_from_filename_patterns(input_file, company_info, thread_id=None):
    return False # Simplified for brevity, original logic can be kept if needed

def general_pattern_extraction(rows, company_info, thread_id=None):
    pass # Simplified for brevity, original logic can be kept if needed

def extract_description(rows, thread_id=None):
    for i, row in enumerate(rows[5:20]):
        for cell in row:
            if cell and isinstance(cell, str) and "S&P Global offers" in cell:
                return cell
    return "S&P Global offers a diverse range of sustainability measurements..."

def identify_all_sections(rows, thread_id=None):
    sections = {}
    current_risk_type = None
    category_counts = defaultdict(int)
    for i, row in enumerate(rows):
        if not row or all(pd.isna(cell) or cell == "" for cell in row): continue
        cell_val = str(row[0]) if pd.notna(row[0]) else ""
        if cell_val in ["Environmental", "Social", "Governance"]:
            current_risk_type = cell_val
            continue
        if cell_val and cell_val not in ["Question", "Response", "Industry Comparison", "Industry Average"]:
            category_name = cell_val
            category_counts[category_name] += 1
            if category_counts[category_name] > 1:
                category_name = f"{category_name}({category_counts[category_name]})"
            sections[i] = {"type": current_risk_type, "name": category_name}
    return sections

def extract_data_entries(rows, sections, thread_id=None):
    data_entries = []
    current_risk_type, current_category = None, None
    section_boundaries = sorted(list(sections.keys()))
    for i, row in enumerate(rows):
        if not row or all(pd.isna(cell) or cell == "" for cell in row): continue
        if i in sections:
            current_risk_type = sections[i]["type"]
            current_category = sections[i]["name"]
            continue
        if isinstance(row[0], str) and row[0] == "Question":
            question = row[1] if len(row) > 1 and pd.notna(row[1]) else ""
            response = rows[i+1][1] if i+1 < len(rows) and len(rows[i+1]) > 1 and pd.notna(rows[i+1][1]) else ""
            comparison = rows[i+2][1] if i+2 < len(rows) and len(rows[i+2]) > 1 and pd.notna(rows[i+2][1]) else ""
            average = rows[i+3][1] if i+3 < len(rows) and len(rows[i+3]) > 1 and pd.notna(rows[i+3][1]) else ""
            data_entries.append({
                'risk_type': current_risk_type or "Unknown",
                'category': current_category or "Unknown",
                'question': question, 'response': response,
                'comparison': comparison, 'average': average
            })
    return data_entries

def create_structured_dataframe(company_name, company_key, description, data_entries):
    rows = []
    for entry in data_entries:
        rows.append({
            'company_name': company_name, 'key': company_key, 'description': description,
            'risk_type': entry['risk_type'], 'category': entry['category'],
            'question': entry['question'], 'response': entry['response'],
            'industry_comparison': entry['comparison'], 'industry_average': entry['average']
        })
    return pd.DataFrame(rows)


# =============================================================================
# CONSOLIDATION AND CSV GENERATION FUNCTIONS
# =============================================================================

def create_csv_friendly_consolidation(df):
    """
    Create a CSV-friendly consolidated view - one row per company per risk type
    with flattened metrics as separate columns.
    """
    logger.info("Creating CSV-friendly consolidation...")
    
    consolidated_df = df.groupBy(
        "company_name", "key", "description", "risk_type"
    ).agg(
        F.first("source_file").alias("source_file"),
        F.first("processed_timestamp").alias("processed_timestamp"),
        # Count metrics per category (flattened)
        F.count(F.when(F.col("category") == "Biodiversity", 1)).alias("biodiversity_metrics_count"),
        F.count(F.when(F.col("category") == "Energy", 1)).alias("energy_metrics_count"),
        F.count(F.when(F.col("category") == "Climate", 1)).alias("climate_metrics_count"),
        F.count(F.when(F.col("category") == "Human Capital", 1)).alias("human_capital_metrics_count"),
        F.count(F.when(F.col("category") == "Employee Health", 1)).alias("employee_health_metrics_count"),
        F.count(F.when(F.col("category") == "Human Rights", 1)).alias("human_rights_metrics_count"),
        F.count(F.when(F.col("category") == "Ethics", 1)).alias("ethics_metrics_count"),
        F.count(F.when(F.col("category") == "Corporate Governance", 1)).alias("corporate_governance_metrics_count"),
        F.count(F.when(F.col("category") == "Risk & Crisis", 1)).alias("risk_crisis_metrics_count"),
        F.count(F.when(F.col("category") == "Employment Practices", 1)).alias("employment_practices_metrics_count"),
        # Total metrics count
        F.count("*").alias("total_metrics_count")
    ).orderBy(
        "company_name",
        F.when(F.col("risk_type") == "Environmental", 1)
         .when(F.col("risk_type") == "Social", 2)
         .when(F.col("risk_type") == "Governance", 3)
         .otherwise(4)
    )
    
    return consolidated_df


def sanitize_column_name(col_name):
    name = str(col_name)
    # Specific replacement for (%) to _pct or similar BEFORE general replacements
    name = name.replace('(%)', '_pct') # Or _percent
    
    # Replace common problematic characters (space, comma, semicolon, braces, parentheses, slash, hyphen, newline, tab, equals) with underscore
    name = re.sub(r'[ ,;{}()\n\t=/%-]+', '_', name) 
    # Remove any character that is not alphanumeric or underscore
    name = re.sub(r'[^A-Za-z0-9_]+', '', name)
    # Consolidate multiple underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores that might result from replacements
    name = name.strip('_')
    
    if not name:
        name = "unnamed_col"
    return name.lower()


def create_unified_csv_files(climate_data, output_file_path):
    """
    Create CSV files using Spark's native capabilities for better performance.
    """
    try:
        if not climate_data or climate_data.count() == 0:
            print("No data to write to CSV")
            return False, "No data available"
        
        # Limit records if dataset is too large
        if climate_data.count() > MAX_RECORDS_PER_FILE:
            print(f"Large dataset detected ({climate_data.count()} records). Truncating to {MAX_RECORDS_PER_FILE} records.")
            climate_data = climate_data.limit(MAX_RECORDS_PER_FILE)
        
        # Generate timestamped filenames
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = output_file_path.rsplit('/', 1)[0]
        
        # 1. Create CONSOLIDATED CSV (Company-RiskType level - flattened for CSV)
        consolidated_df = create_csv_friendly_consolidation(climate_data)
        consolidated_count = consolidated_df.count()
        consolidated_headers = consolidated_df.columns
        
        print(f"📊 CONSOLIDATED CSV Stats:")
        print(f"   Rows: {consolidated_count:,}")
        print(f"   Columns: {len(consolidated_headers)}")
        print(f"   Headers: {consolidated_headers}")
        
        consolidated_csv_path = f"{base_path}/consolidated_climate_esg_data_{timestamp}.csv"
        temp_consolidated_dir = f"{consolidated_csv_path}_temp"
        consolidated_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_consolidated_dir)
        
        # Find the part file and rename it to the final CSV
        part_files = dbutils.fs.ls(temp_consolidated_dir)
        for part_file in part_files:
            if part_file.name.startswith("part-") and part_file.name.endswith(".csv"):
                dbutils.fs.cp(part_file.path, consolidated_csv_path)
                break
        
        # Clean up temp directory
        dbutils.fs.rm(temp_consolidated_dir, True)
        print(f"✓ Created consolidated CSV: {consolidated_csv_path}")
        
        # 2. Create CLIMATE CSV (flattened data - one row per question/response)
        climate_count = climate_data.count()
        climate_headers = climate_data.columns
        
        print(f"📊 CLIMATE CSV Stats:")
        print(f"   Rows: {climate_count:,}")
        print(f"   Columns: {len(climate_headers)}")
        print(f"   Headers: {climate_headers}")
        
        climate_csv_path = f"{base_path}/climate_data_{timestamp}.csv"
        temp_climate_dir = f"{climate_csv_path}_temp"
        climate_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_climate_dir)
        
        # Find the part file and rename it to the final CSV
        part_files = dbutils.fs.ls(temp_climate_dir)
        for part_file in part_files:
            if part_file.name.startswith("part-") and part_file.name.endswith(".csv"):
                dbutils.fs.cp(part_file.path, climate_csv_path)
                break
        
        # Clean up temp directory
        dbutils.fs.rm(temp_climate_dir, True)
        print(f"✓ Created climate data CSV: {climate_csv_path}")
        
        # Summary comparison
        reduction_percentage = ((climate_count - consolidated_count) / climate_count * 100) if climate_count > 0 else 0
        print(f"\n📈 CONSOLIDATION SUMMARY:")
        print(f"   Original data: {climate_count:,} rows")
        print(f"   Consolidated: {consolidated_count:,} rows")
        print(f"   Reduction: {reduction_percentage:.1f}%")
        print(f"   Compression ratio: {climate_count/consolidated_count:.1f}:1")
        
        return True, consolidated_csv_path
        
    except Exception as e:
        error_msg = f"Error creating CSV files: {str(e)}"
        print(f"✗ {error_msg}")
        return False, error_msg

# =============================================================================
# MAIN PROCESSING FUNCTIONS
# =============================================================================

def setup_directories():
    """Create necessary directories if they don't exist and initialize tracking file"""
    for directory in [OUTPUT_DIR, ERROR_DIR, LOG_DIR]:
        dbutils.fs.mkdirs(directory)
        logger.info(f"Ensured directory exists: {directory}")
    
    tracking_file = f"{OUTPUT_DIR}/processed_files.txt"
    try:
        dbutils.fs.ls(tracking_file)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            logger.info(f"Tracking file {tracking_file} not found, creating it.")
            content = "# S&P Global Climate processed files\n"
            content += "# Format: absolute_path_to_file\n"
            dbutils.fs.put(tracking_file, content, True)
            logger.info(f"Created tracking file: {tracking_file}")
        else:
            logger.error(f"Error checking tracking file {tracking_file}: {str(e)}")
            raise
    return tracking_file


def get_processed_files(tracking_file):
    """Read the list of already processed files from the tracking file"""
    processed_files = set()
    try:
        dbutils.fs.ls(tracking_file) 
        content = dbutils.fs.head(tracking_file, 1024 * 1024 * 10) # Read up to 10MB
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                processed_files.add(line)
        logger.info(f"Found {len(processed_files)} previously processed files in tracking file: {tracking_file}")
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            logger.info(f"Tracking file {tracking_file} not found. Starting with an empty set of processed files.")
        else:
            logger.error(f"Error reading tracking file {tracking_file}: {str(e)}. Proceeding with an empty set.")
    return processed_files


def process_excel_file_wrapper(file_path_arg, error_dir_arg, max_retries_arg):
    """Wrapper for extract_and_process_file to be used with ThreadPoolExecutor."""
    file_name = file_path_arg.split("/")[-1]
    current_thread_name = threading.current_thread().name
    logger.info(f"Thread {current_thread_name} starting processing for: {file_name}")
    
    success = False
    error_message = ""
    data_payload = None

    for attempt in range(max_retries_arg):
        try:
            if attempt > 0:
                wait_time = random.uniform(0.5, 1.5) * (attempt + 1)
                logger.info(f"Retrying file {file_name} (attempt {attempt + 1}/{max_retries_arg}) after {wait_time:.2f}s delay. Thread: {current_thread_name}")
                time.sleep(wait_time)
            
            success, file_path, data_payload = extract_and_process_file(file_path_arg)
            
            if success:
                logger.info(f"Thread {current_thread_name} successfully extracted data from: {file_name}")
                return True, file_path_arg, data_payload, None  # success, filepath, data, no error
            else:
                logger.warning(f"Thread {current_thread_name} attempt {attempt + 1} failed for {file_name}: {data_payload}")
        except Exception as e:
            error_message = f"Unexpected exception on attempt {attempt + 1} for {file_name} in thread {current_thread_name}: {str(e)}"
            logger.error(error_message)
            logger.debug(traceback.format_exc())
            success = False
            data_payload = None # Ensure no stale data

        if success: break # Should be caught by return True above
            
    if not success:
        logger.error(f"File {file_name} failed after {max_retries_arg} attempts by thread {current_thread_name}. Final error: {error_message}")
        try:
            dbutils.fs.cp(file_path_arg, f"{error_dir_arg}/{file_name}", recurse=False)
            logger.info(f"Thread {current_thread_name} copied failed file {file_name} to {error_dir_arg}")
        except Exception as copy_e:
            logger.error(f"Thread {current_thread_name} failed to copy {file_name} to error directory {error_dir_arg}: {copy_e}")
        return False, file_path_arg, None, error_message # failure, filepath, no data, error
    
    # Fallback, ideally not reached if success path returns earlier
    return True, file_path_arg, data_payload, None


# =============================================================================
# CONSOLIDATION LOGIC
# =============================================================================

# =============================================================================
# MAIN EXECUTION
# =============================================================================
import pandas as pd
import re
import os
import concurrent.futures
import time
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType
import datetime
from collections import defaultdict

# Modify extraction functions to include thread_id for better logging
def extract_company_info(rows, input_file=None, thread_id=None):
    """
    Extract company name and key information using pattern matching.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    company_info = {'company_name': '', 'company_key': ''}
    
    print(f"{log_prefix}Attempting company info extraction using pattern matching...")
    
    # Strategy 1: Extract from Excel content first (most reliable)
    found_in_content = extract_from_excel_content_patterns(rows, company_info, thread_id)
    
    # If we found both name and key, we're done
    if found_in_content and company_info['company_name'] and company_info['company_key']:
        print(f"{log_prefix}Successfully extracted from Excel content")
        return company_info
    
    # Strategy 2: If not found or incomplete, try from filename
    if input_file:
        extract_from_filename_patterns(input_file, company_info, thread_id)
        
        # If we now have both, we're done
        if company_info['company_name'] and company_info['company_key']:
            print(f"{log_prefix}Successfully extracted from filename")
            return company_info
    
    # Strategy 3: Try more general pattern recognition in content
    if not company_info['company_name'] or not company_info['company_key']:
        general_pattern_extraction(rows, company_info, thread_id)
        
    # Set defaults if nothing was found
    if not company_info['company_name']:
        company_info['company_name'] = "Unknown Company"
        print(f"{log_prefix}WARNING: Could not extract company name. Using default.")
    
    print(f"{log_prefix}Final extraction results - Name: {company_info['company_name']}, Key: {company_info['company_key']}")
    return company_info


def extract_from_excel_content_patterns(rows, company_info, thread_id=None):
    """
    Extract company info from Excel content using pattern matching.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    print(f"{log_prefix}Looking for company info patterns in Excel content...")
    found_any = False
    
    # Look for potential company information in the first 20 rows
    for i, row in enumerate(rows[:20]):
        for j, cell in enumerate(row):
            if not cell or not isinstance(cell, str):
                continue
                
            cell_text = str(cell).strip()
            
            # Skip if too short
            if len(cell_text) < 10:
                continue
                
            # Pattern 1: Look for text with a pipe symbol (often separates company info from description)
            if " | " in cell_text:
                print(f"{log_prefix}Found potential company info with pipe: {cell_text}")
                company_part = cell_text.split(" | ")[0].strip()
                
                # Now parse the company part to extract name and key
                extract_from_company_part(company_part, company_info, thread_id)
                found_any = True
                continue
            
            # Pattern 2: Look for text with parentheses (often contains key identifiers)
            if "(" in cell_text and ")" in cell_text:
                # Check if this looks like a company info cell (has capital letters followed by colon)
                if re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', cell_text):
                    print(f"{log_prefix}Found potential company info with parentheses: {cell_text}")
                    extract_from_company_part(cell_text, company_info, thread_id)
                    found_any = True
                    continue
            
            # Pattern 3: Look for exchange:symbol pattern
            match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', cell_text)
            if match:
                # This might be a cell with company name and key
                print(f"{log_prefix}Found potential exchange:symbol pattern: {cell_text}")
                extract_from_company_part(cell_text, company_info, thread_id)
                found_any = True
    
    return found_any


# def extract_from_company_part(text, company_info, thread_id=None):
#     """
#     Extract company name and key from text that appears to contain company information.
#     """
#     log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
#     # Use a more precise regex to capture the FULL ticker including all digits
#     exchange_symbol_match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', text)
    
#     if exchange_symbol_match:
#         exchange_symbol = exchange_symbol_match.group(1)
#         parts = text.split(exchange_symbol)

#         # Extract name
#         potential_name = parts[0].strip()
#         potential_name = re.sub(r'[,\.;:]$', '', potential_name).strip()

#         if not company_info['company_name'] and potential_name:
#             company_info['company_name'] = potential_name
#             print(f"{log_prefix}[✓] Extracted company name: {company_info['company_name']}")

#         # Extract content inside the first pair of parentheses
#         paren_match = re.search(r'\(([^)]+)\)', text)
#         if paren_match:
#             id_block = paren_match.group(0)  # includes ()
#             full_key = f"{exchange_symbol} {id_block}"
#         else:
#             full_key = exchange_symbol

#         if not company_info['company_key']:
#             company_info['company_key'] = full_key
#             print(f"{log_prefix}[✓] Extracted company key: {company_info['company_key']}")
        # else:
            # # Fallback: no exchange symbol found
            # paren_match = re.search(r'\(([^)]+)\)', text)
            # if paren_match:
            #     name_part = text.split('(')[0].strip()
            #     key_part = paren_match.group(0)  # includes ()
                
            #     if not company_info['company_name'] and name_part:
            #         company_info['company_name'] = name_part
            #         print(f"{log_prefix}[✓] Extracted fallback company name: {company_info['company_name']}")

            #     if not company_info['company_key']:
            #         company_info['company_key'] = key_part
            #         print(f"{log_prefix}[✓] Extracted fallback company key: {company_info['company_key']}")

def extract_from_company_part(text, company_info, thread_id=None):
    """
    Extract company name and key from text that appears to contain company information.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
    # Use a more precise regex to capture the FULL ticker including all digits
    exchange_symbol_match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', text)
    
    if exchange_symbol_match:
        exchange_symbol = exchange_symbol_match.group(1)
        parts = text.split(exchange_symbol)

        # Extract name
        potential_name = parts[0].strip()
        potential_name = re.sub(r'[,\.;:]$', '', potential_name).strip()

        if not company_info['company_name'] and potential_name:
            company_info['company_name'] = potential_name
            print(f"{log_prefix}[✓] Extracted company name: {company_info['company_name']}")

        # Find all parentheses
        paren_matches = list(re.finditer(r'\(([^)]+)\)', text))
        
        # If there are multiple parentheses, prefer the last one
        if len(paren_matches) > 1:
            # Get the last parenthesis which usually contains identifiers
            id_block = paren_matches[-1].group(0)  # includes ()
            full_key = f"{exchange_symbol} {id_block}"
        elif len(paren_matches) == 1:
            # Only one parenthesis found
            id_block = paren_matches[0].group(0)  # includes ()
            full_key = f"{exchange_symbol} {id_block}"
        else:
            # No parentheses found
            full_key = exchange_symbol

        if not company_info['company_key']:
            company_info['company_key'] = full_key
            print(f"{log_prefix}[✓] Extracted company key: {company_info['company_key']}")

    else:
        # Fallback: no exchange symbol found
        paren_matches = list(re.finditer(r'\(([^)]+)\)', text))
        
        if len(paren_matches) > 0:
            # Prefer the last parenthesis if multiple are found
            if len(paren_matches) > 1:
                # Get the position of the last parenthesis for name extraction
                name_part = text[:paren_matches[-1].start()].strip()
                key_part = paren_matches[-1].group(0)  # includes ()
            else:
                # Only one parenthesis
                name_part = text.split('(')[0].strip()
                key_part = paren_matches[0].group(0)  # includes ()
            
            if not company_info['company_name'] and name_part:
                company_info['company_name'] = name_part
                print(f"{log_prefix}[✓] Extracted fallback company name: {company_info['company_name']}")

            if not company_info['company_key']:
                company_info['company_key'] = key_part
                print(f"{log_prefix}[✓] Extracted fallback company key: {company_info['company_key']}")

def extract_from_filename_patterns(input_file, company_info, thread_id=None):
    """
    Extract company information from filename using pattern matching.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
    if not input_file:
        return False
        
    filename = os.path.basename(input_file)
    print(f"{log_prefix}Trying to extract company info from filename: {filename}")
    
    # Special case for SPGlobal format
    if filename.startswith("SPGlobal_"):
        # Pattern: SPGlobal_CompanyName with various separators
        parts = filename.split('_')
        if len(parts) < 2:
            return False
        
        # Get the company name part (after SPGlobal_)
        company_part = parts[1]
        
        # Get the raw name (before any period or underscore)
        raw_name = company_part
        if '.' in company_part:
            raw_name = company_part.split('.')[0]
        
        # Format company name: Add spaces before capital letters in CamelCase
        formatted_name = re.sub(r'(?<!^)(?=[A-Z])', ' ', raw_name).strip()
        
        # Clean up common suffixes
        if formatted_name.lower().endswith("inc."):
            formatted_name = formatted_name[:-4].strip()
        elif formatted_name.lower().endswith("inc"):
            formatted_name = formatted_name[:-3].strip()
        elif formatted_name.lower().endswith("corporation"):
            formatted_name = formatted_name[:-11].strip()
        
        # Set the company name
        if not company_info['company_name']:
            company_info['company_name'] = formatted_name
            print(f"{log_prefix}Extracted company name from filename: {formatted_name}")
        
        # Look for exchange/ticker pattern - try multiple formats
        # First try looking in the company_part after a period
        exchange_ticker = ""
        if '.' in company_part:
            exchange_ticker = company_part.split('.')[1]
        # If not found, check next part after underscore
        elif len(parts) >= 3:
            exchange_ticker = parts[2]
        
        # Try to find Exchange and Ticker using regex
        exchange_match = None
        if exchange_ticker:
            # Try to match patterns like NASDAQGSAAPL or NYSEGS-ABC
            exchange_match = re.search(r'(NASDAQ|NYSE)([A-Z]+)([A-Z]{1,5})', exchange_ticker)
        
        # If that didn't work, try searching the whole filename
        if not exchange_match:
            exchange_match = re.search(r'(NASDAQ|NYSE)([A-Z]+)([A-Z]{1,5})', filename)
        
        if exchange_match:
            exchange = exchange_match.group(1) + exchange_match.group(2)
            ticker = exchange_match.group(3)
            
            # Extract ID values from the entire filename
            mikey_match = re.search(r'MIKEY(\d+)', filename)
            spciq_match = re.search(r'SPCIQKEY(\d+)', filename)
            tcuid_match = re.search(r'TCUID(\d+)', filename)
            
            # Build the key
            key_parts = []
            key_parts.append(f"{exchange}:{ticker}")
            
            if mikey_match:
                key_parts.append(f"MI KEY: {mikey_match.group(1)}")
            
            if spciq_match:
                key_parts.append(f"SPCIQ KEY: {spciq_match.group(1)}")
            
            if tcuid_match:
                key_parts.append(f"TCUID: {tcuid_match.group(1)}")
            
            # Format final key
            if key_parts and not company_info['company_key']:
                company_info['company_key'] = key_parts[0]
                if len(key_parts) > 1:
                    company_info['company_key'] += f" ({'; '.join(key_parts[1:])})"
                
                print(f"{log_prefix}Extracted company key from filename: {company_info['company_key']}")
                return True
    
    # For other filename patterns (non-SPGlobal)
    # Look for patterns like Report_Company_Name_EXCHANGE_TICKER or CompanyData_Name_TICKER
    parts = filename.split('_')
    if len(parts) >= 3:
        # Try to identify company name part
        potential_name_parts = []
        for i, part in enumerate(parts[1:-1]):  # Skip first and last part
            # If it contains exchange or ticker pattern, stop
            if re.search(r'(NASDAQ|NYSE|[A-Z]{2,5}$)', part):
                break
            potential_name_parts.append(part)
        
        if potential_name_parts:
            # Format company name
            formatted_name = ' '.join(potential_name_parts)
            formatted_name = re.sub(r'([A-Z])', r' \1', formatted_name).strip()
            formatted_name = re.sub(r'\s+', ' ', formatted_name)  # Fix multiple spaces
            
            if not company_info['company_name']:
                company_info['company_name'] = formatted_name
                print(f"{log_prefix}Extracted company name from non-SPGlobal filename: {formatted_name}")
            
            # Look for exchange/ticker pattern
            for part in parts:
                # Check for NYSE or NASDAQ
                exchange_match = re.search(r'(NYSE|NASDAQ)', part)
                if exchange_match:
                    exchange = exchange_match.group(1)
                    
                    # Look for ticker in next part or same part
                    ticker = ""
                    ticker_index = parts.index(part) + 1
                    if ticker_index < len(parts):
                        ticker_part = parts[ticker_index]
                        if re.match(r'^[A-Z]{1,5}$', ticker_part):
                            ticker = ticker_part
                    
                    # If ticker not found in next part, try same part
                    if not ticker:
                        ticker_match = re.search(r'[A-Z]{2,5}$', part)
                        if ticker_match:
                            ticker = ticker_match.group(0)
                    
                    if ticker:
                        company_info['company_key'] = f"{exchange}:{ticker}"
                        print(f"{log_prefix}Extracted company key from non-SPGlobal filename: {company_info['company_key']}")
                        return True
    
    return False


def general_pattern_extraction(rows, company_info, thread_id=None):
    """
    Use general pattern recognition when more specific methods fail.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    print(f"{log_prefix}Attempting general pattern extraction...")
    
    # Look for cells that might contain company information based on patterns
    for i, row in enumerate(rows[:30]):
        for j, cell in enumerate(row):
            if not cell or not isinstance(cell, str):
                continue
                
            cell_text = str(cell).strip()
            
            # Skip short texts
            if len(cell_text) < 10:
                continue
                
            # Look for capital letters followed by colon and more capital letters (e.g., NYSE:ABC)
            exchange_symbol_match = re.search(r'([A-Z]{2,}:\d*[A-Z0-9]+)', cell_text)
            if exchange_symbol_match and not company_info['company_key']:
                exchange_symbol = exchange_symbol_match.group(1)
                company_info['company_key'] = exchange_symbol
                print(f"{log_prefix}Found potential company key from pattern: {exchange_symbol}")
                
                # Try to extract company name from the same cell
                if exchange_symbol in cell_text:
                    name_part = cell_text.split(exchange_symbol)[0].strip()
                    if name_part and not company_info['company_name']:
                        # Clean up the name
                        name_part = re.sub(r'[,\.;:]$', '', name_part).strip()
                        company_info['company_name'] = name_part
                        print(f"{log_prefix}Found potential company name from same cell: {name_part}")
            
            # Look for parentheses with key:value pairs
            parentheses_match = re.search(r'\((.*?(KEY|ID|IDENTIFIER).*?)\)', cell_text, re.IGNORECASE)
            if parentheses_match and not company_info['company_key']:
                key_info = parentheses_match.group(0)
                company_info['company_key'] = key_info
                print(f"{log_prefix}Found potential company key from parentheses: {key_info}")
                
                # Try to extract company name from the same cell
                if key_info in cell_text:
                    name_part = cell_text.split(key_info)[0].strip()
                    if name_part and not company_info['company_name']:
                        company_info['company_name'] = name_part
                        print(f"{log_prefix}Found potential company name from same cell: {name_part}")
    
    # If we still don't have a company name, look for cells that might be company names
    if not company_info['company_name']:
        for i, row in enumerate(rows[:20]):
            for cell in row:
                if not cell or not isinstance(cell, str):
                    continue
                    
                cell_text = str(cell).strip()
                
                # Check if this looks like a standalone company name
                if (len(cell_text) > 5 and len(cell_text) < 50 and 
                    cell_text[0].isupper() and ' ' in cell_text and
                    not any(char in cell_text for char in [':', '#', '@', '%', '|', '=']) and
                    not cell_text.startswith("S&P Global")):
                    
                    # Further validate - it shouldn't be just a category name
                    if not any(word in cell_text.lower() for word in [
                        'summary', 'overview', 'sustainability', 'climate', 'environment', 
                        'social', 'governance', 'index', 'question', 'average'
                    ]):
                        company_info['company_name'] = cell_text
                        print(f"{log_prefix}Extracted potential company name from standalone text: {cell_text}")
                        break
            
            # Stop if we found a name
            if company_info['company_name']:
                break


def extract_description(rows, thread_id=None):
    """
    Extract the description text.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
    for i, row in enumerate(rows[5:20]):  # Check rows 5-20
        for cell in row:
            if cell and isinstance(cell, str) and "S&P Global offers" in cell:
                print(f"{log_prefix}Found description in row {i+5+1}")
                return cell
    
    # Default description if not found
    print(f"{log_prefix}Using default description")
    return "S&P Global offers a diverse range of sustainability measurements and workflow solutions. The themes are a selection of company specific measurements that are essential for measuring performance and progress on sustainability topics. The measurements are derived from a broad ecosystem of proprietary datasets that include corporate sustainability assessments (CSA), environmental and climate risk measurements."


def identify_all_sections(rows, thread_id=None):
    """
    Pre-analyze the rows to identify all sections and their types.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
    sections = {}
    current_risk_type = None
    
    # Keep track of category occurrences
    category_counts = defaultdict(int)
    
    for i, row in enumerate(rows):
        # Skip empty rows
        if not row or all(cell is None or cell == "" or (isinstance(cell, str) and cell.strip() == "") for cell in row):
            continue
        
        # Check for risk type headers
        if isinstance(row[0], str):
            if row[0] == "Environmental":
                current_risk_type = "Environmental"
                print(f"{log_prefix}Found Environmental section at row {i+1}")
                continue
            elif row[0] == "Social":
                current_risk_type = "Social"
                print(f"{log_prefix}Found Social section at row {i+1}")
                continue
            elif row[0] == "Governance":
                current_risk_type = "Governance"
                print(f"{log_prefix}Found Governance section at row {i+1}")
                continue
        
        # Check for section headers
        if row[0] and isinstance(row[0], str):
            # Skip rows that are part of the data structure (Question, Response, etc.)
            if row[0] in ["Question", "Response", "Industry Comparison", "Industry Average"]:
                continue
                
            # Handle all section headers
            if any(keyword in row[0] for keyword in [
                "Biodiversity", "Energy", "Climate", "Human Capital", 
                "Employee Health", "Human Rights", "Ethics", 
                "Corporate Governance", "Risk & Crisis", "Employment Practices"
            ]):
                # Count occurrences of this category
                category_name = row[0]
                category_counts[category_name] += 1
                
                # If it's a duplicate category, add numbering
                if category_counts[category_name] > 1:
                    category_name = f"{category_name}({category_counts[category_name]})"
                elif category_counts[category_name] == 1 and any(
                    category_name == cat.split('(')[0] for cat in category_counts.keys() 
                    if '(' in cat
                ):
                    # First occurrence, but we know there will be more, so add (1)
                    category_name = f"{category_name}(1)"
                
                sections[i] = {
                    "type": current_risk_type,
                    "name": category_name
                }
                print(f"{log_prefix}Found section: {current_risk_type} - {category_name} at row {i+1}")
    
    print(f"{log_prefix}Identified sections:")
    for idx, section in sections.items():
        print(f"{log_prefix}  Row {idx+1}: {section['type']} - {section['name']}")
    
    return sections


def extract_data_entries(rows, sections, thread_id=None):
    """
    Extract all data entries into a structured format.
    """
    log_prefix = f"Thread {thread_id}: " if thread_id else ""
    
    data_entries = []
    current_risk_type = None
    current_category = None
    
    # Create a reverse lookup to find which section a row belongs to
    section_boundaries = list(sections.keys())
    section_boundaries.sort()
    
    for i, row in enumerate(rows):
        # Skip empty rows
        if not row or all(cell is None or cell == "" or (isinstance(cell, str) and cell.strip() == "") for cell in row):
            continue
        
        # Check for risk type headers
        if isinstance(row[0], str):
            if row[0] == "Environmental":
                current_risk_type = "Environmental"  
                continue
            elif row[0] == "Social":
                current_risk_type = "Social"
                continue
            elif row[0] == "Governance":
                current_risk_type = "Governance"
                continue
        
        # Check if we're at a section header
        if i in sections:
            current_category = sections[i]["name"]
            continue
        # Check if we need to find which section this row belongs to
        elif current_category is None or current_risk_type is None:
            # Find the most recent section header before this row
            for boundary in reversed(section_boundaries):
                if boundary < i:
                    current_category = sections[boundary]["name"]
                    current_risk_type = sections[boundary]["type"]
                    break
        
        # Check for Question-Response pattern
        if i+3 < len(rows) and isinstance(row[0], str) and row[0] == "Question":
            question = row[1] if len(row) > 1 and row[1] else "Unknown question"
            
            # Look for Response, Industry Comparison, and Industry Average rows
            response_row = rows[i+1] if i+1 < len(rows) else None
            comparison_row = rows[i+2] if i+2 < len(rows) else None
            average_row = rows[i+3] if i+3 < len(rows) else None
            
            if (response_row and len(response_row) > 0 and isinstance(response_row[0], str) and response_row[0] == "Response" and 
                comparison_row and len(comparison_row) > 0 and isinstance(comparison_row[0], str) and comparison_row[0] == "Industry Comparison" and
                average_row and len(average_row) > 0 and isinstance(average_row[0], str) and average_row[0] == "Industry Average"):
                
                # Create a data entry
                response_value = response_row[1] if len(response_row) > 1 and response_row[1] is not None else ""
                comparison_value = comparison_row[1] if len(comparison_row) > 1 and comparison_row[1] is not None else ""
                average_value = average_row[1] if len(average_row) > 1 and average_row[1] is not None else ""
                
                data_entries.append({
                    'risk_type': current_risk_type if current_risk_type else "Unknown",
                    'category': current_category if current_category else "Unknown Category",
                    'question': question,
                    'response': response_value,
                    'comparison': comparison_value,
                    'average': average_value
                })
    
    return data_entries


def create_structured_dataframe(company_name, company_key, description, data_entries):
    """
    Create a pandas DataFrame with the structured data.
    """
    # Create a list of dictionaries for each row
    rows = []
    
    for entry in data_entries:
        row = {
            'company_name': company_name,
            'key': company_key,
            'description': description,
            'risk_type': entry['risk_type'],
            'category': entry['category'],
            'question': entry['question'],
            'response': entry['response'],
            'industry_comparison': entry['comparison'],
            'industry_average': entry['average']
        }
        rows.append(row)
    
    # Add placeholders for Social section if not present
    if not any(entry['risk_type'] == 'Social' for entry in data_entries):
        # Add placeholders
        for i in range(4):
            rows.append({
                'company_name': company_name,
                'key': company_key,
                'description': description,
                'risk_type': 'Social',
                'category': '',
                'question': '',
                'response': '',
                'industry_comparison': '',
                'industry_average': ''
            })
    
    # Add placeholders for Governance section if not present
    if not any(entry['risk_type'] == 'Governance' for entry in data_entries):
        # Add placeholders
        for i in range(4):
            rows.append({
                'company_name': company_name,
                'key': company_key,
                'description': description,
                'risk_type': 'Governance',
                'category': '',
                'question': '',
                'response': '',
                'industry_comparison': '',
                'industry_average': ''
            })
    
    # Create DataFrame
    return pd.DataFrame(rows)




# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    tracking_file = setup_directories()
    processed_files_set_on_start = get_processed_files(tracking_file)
    newly_processed_in_this_run = set()
    
    # Get list of files to process
    file_list_from_dbfs = dbutils.fs.ls(INPUT_DIR)
    files_to_process_this_run = []
    
    for file_info in file_list_from_dbfs:
        if file_info.isDir(): 
            continue
        
        file_path = file_info.path
        file_name = file_info.name 
        fn_lower = file_name.lower()
        is_excel = any(fn_lower.endswith(ext) for ext in SUPPORTED_EXTENSIONS)
        contains_keyword = FILE_KEYWORD in fn_lower 
        
        if is_excel and contains_keyword:
            if file_path not in processed_files_set_on_start:
                files_to_process_this_run.append(file_path)
            
    if not files_to_process_this_run:
        print("No new Excel files found to process.")
        return
    
    print(f"Found {len(files_to_process_this_run)} files to process using distributed Spark.")
    
    # Test a single file first to debug issues
    if files_to_process_this_run:
        test_file = files_to_process_this_run[0]
        print(f"Testing first file: {test_file}")
        try:
            success, processed_fp, data_df, error_msg = extract_and_process_file(test_file)
            print(f"Test result - Success: {success}, Error: {error_msg}")
            if success and data_df is not None:
                print(f"Test data shape: {data_df.shape}")
                print(f"Test data columns: {list(data_df.columns)}")
        except Exception as e:
            print(f"Test file failed with exception: {e}")
            import traceback
            traceback.print_exc()
    
    # Get cluster info for optimal batch sizing
    try:
        executor_cores = int(spark.conf.get("spark.executor.cores", "2"))
        executor_instances = int(spark.conf.get("spark.executor.instances", "2"))
        total_cores = executor_cores * executor_instances
        print(f"Cluster: {executor_instances} executors x {executor_cores} cores = {total_cores} total cores")
    except:
        total_cores = 4
        print(f"Using default cluster size: {total_cores} cores")
    
    # Use Spark RDD for distributed processing (like sp_business.py pattern)
    USE_SPARK_DISTRIBUTION = len(files_to_process_this_run) > 50  # Use Spark for datasets > 50 files
    
    if USE_SPARK_DISTRIBUTION:
        print(f"🚀 Large dataset detected ({len(files_to_process_this_run)} files) - using Spark RDD distributed processing")
        
        def spark_process_file(file_path):
            """Function to be executed on Spark executors"""
            import os
            import time
            import random
            
            file_name = file_path.split("/")[-1]
            
            for attempt in range(MAX_RETRIES):
                try:
                    if attempt > 0:
                        wait_time = random.uniform(0.5, 1.5) * (attempt + 1)
                        time.sleep(wait_time)
                    
                    success, processed_fp, data_df, error_msg = extract_and_process_file(file_path)
                    
                    if success and data_df is not None:
                        return (True, file_path, data_df, None)
                    else:
                        if attempt == MAX_RETRIES - 1:  # Last attempt
                            return (False, file_path, None, error_msg)
                        
                except Exception as e:
                    error_message = f"Spark executor exception for {file_name}: {str(e)}"
                    if attempt == MAX_RETRIES - 1:  # Last attempt
                        return (False, file_path, None, error_message)
            
            return (False, file_path, None, "Max retries exceeded")
        
        # Create RDD and process files across cluster with optimal partitioning
        # Use more aggressive partitioning for better parallelization
        optimal_partitions = min(max(total_cores * 12, 600), len(files_to_process_this_run) // 20)
        print(f"Creating RDD with {optimal_partitions} partitions for {len(files_to_process_this_run)} files")
        print(f"Target: ~{len(files_to_process_this_run) // optimal_partitions} files per partition")
        
        files_rdd = spark.sparkContext.parallelize(files_to_process_this_run, numSlices=optimal_partitions)
        results_rdd = files_rdd.map(spark_process_file)
        
        # Cache the RDD for better performance
        results_rdd.cache()
        
        # Force evaluation to trigger Spark job
        print("Triggering Spark job execution...")
        partition_count = results_rdd.getNumPartitions()
        print(f"RDD has {partition_count} partitions")
        
        # Collect results with progress tracking (reverted to reliable approach)
        print("Collecting results from Spark executors...")
        print(f"Processing {len(files_to_process_this_run)} files across {optimal_partitions} partitions")
        results = results_rdd.collect()
        
        # Process results
        all_climate_dataframes = []
        total_successful_files = 0
        total_failed_files = []
        
        for result in results:
            success, file_path, data_df, error_msg = result
            
            if success and data_df is not None:
                newly_processed_in_this_run.add(file_path)
                total_successful_files += 1
                all_climate_dataframes.append(data_df)
            else:
                total_failed_files.append((file_path.split('/')[-1], error_msg))
        
        print(f"Spark processing complete: {total_successful_files} success, {len(total_failed_files)} failed")
        
    else:
        print(f"📝 Small dataset ({len(files_to_process_this_run)} files) - using multi-threading on driver")
        
        # Use optimized threading with cluster-aware worker count
        max_workers = min(MAX_WORKERS, max(8, total_cores * 4))  # More aggressive parallelism
        print(f"Processing {len(files_to_process_this_run)} files using {max_workers} workers")
        
        # Process files in batches using ThreadPoolExecutor
        all_climate_dataframes = []
        total_successful_files = 0
        total_failed_files = []
        
        # Process in smaller batches to avoid memory issues
        batch_size = min(100, len(files_to_process_this_run) // max_workers + 1)
        total_batches = (len(files_to_process_this_run) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(files_to_process_this_run))
            current_batch = files_to_process_this_run[start_idx:end_idx]
            
            print(f"Processing batch {batch_idx+1}/{total_batches} ({len(current_batch)} files)")
            
            # Process this batch
            successful_batch_files = 0
            failed_batch_files = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="S&P_Climate_FileWorker") as executor:
                future_to_filepath = {
                    executor.submit(process_excel_file_wrapper, fp, ERROR_DIR, MAX_RETRIES): fp
                    for fp in current_batch
                }
                
            for future in concurrent.futures.as_completed(future_to_filepath):
                original_filepath = future_to_filepath[future]
                original_filename = original_filepath.split('/')[-1]
                try:
                    was_success, processed_fp, returned_data_df, error_msg = future.result()
                    
                    if was_success and returned_data_df is not None:
                        newly_processed_in_this_run.add(processed_fp)
                        successful_batch_files += 1
                        all_climate_dataframes.append(returned_data_df)
                    else:
                        # Handle error case
                        failed_batch_files.append((original_filename, error_msg))
                except Exception as exc:
                    failed_batch_files.append((original_filename, str(exc)))
                    try: 
                        dbutils.fs.cp(original_filepath, f"{ERROR_DIR}/{original_filename}", recurse=False)
                    except Exception as copy_exc:
                        pass
            
            # Update totals
            total_successful_files += successful_batch_files
            total_failed_files.extend(failed_batch_files)
            
            print(f"Batch {batch_idx+1} complete: {successful_batch_files} success, {len(failed_batch_files)} failed")
        
        print(f"Threading processing complete: {total_successful_files} success, {len(total_failed_files)} failed")
    
    # Generate unified CSV file after all processing
    if all_climate_dataframes:
        print("Generating CSV files...")
        
        # Combine all pandas DataFrames into one
        combined_pandas_df = pd.concat(all_climate_dataframes, ignore_index=True)
        raw_spark_df = spark.createDataFrame(combined_pandas_df)

        # Generate timestamped CSV filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_output_path = f"{OUTPUT_DIR}/consolidated_climate_esg_data_{timestamp}.csv"
        
        # Write consolidated data to CSV
        success, result = create_unified_csv_files(
            raw_spark_df, 
            csv_output_path
        )
        
        
        if success:
            print(f"✓ CSV files created: {csv_output_path}")
        else:
            print(f"✗ Failed to generate CSV files: {result}")
    
    # Update tracking file
    if newly_processed_in_this_run:
        final_processed_set = processed_files_set_on_start.union(newly_processed_in_this_run)
        tracking_content = "# S&P Global Climate processed files\n# Format: absolute_path_to_file\n"
        for f_path in sorted(list(final_processed_set)): 
            tracking_content += f"{f_path}\n"
        try:
            dbutils.fs.put(tracking_file, tracking_content, True)
        except Exception as e:
            print(f"Error updating tracking file: {e}")

    print(f"Successfully updated tracking file: {tracking_file}")
    #except Exception as e:
        #p#rint(f"Error updating tracking file: {e}")

    # Final summary
    print(f"\n=== PROCESSING COMPLETE ===")
    print(f"Files processed: {total_successful_files}")
    print(f"Files failed: {len(total_failed_files)}")
    if all_climate_dataframes:
        total_records = sum(len(df) for df in all_climate_dataframes)
        print(f"Total records: {total_records:,}")
        print(f"CSV files created in: {OUTPUT_DIR}")
    print("=" * 30)


if __name__ == "__main__":
    main()
