# Databricks notebook source
"""
S&P Global Business Involvement Screens Processor for Databricks

This script processes S&P Global Business Involvement Screens Excel files,
extracts data for three categories, aggregates this data from all files,
and generates unified Excel output files.

FEATURES:
- Reads from Azure Blob Storage
- Generates combined Excel file with single unified sheet
- Creates separate Excel files for individual data types (Company Details, BIS, PS)
- Parallel processing with error handling and retry logic
- Comprehensive logging and file tracking

Requirements:
    - pandas
    - openpyxl
    - xlrd
    - dbutils (provided by Databricks runtime)

Azure Blob Storage Configuration:
    Update INPUT_DIR and OUTPUT_DIR with your actual Azure storage account and container paths.
"""

import os
import re
import pandas as pd
import traceback
from pathlib import Path
import openpyxl # For .xlsx
import xlrd     # For .xls
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
# import gc                 # For garbage collection
from pyspark.sql import SparkSession  # For distributed processing
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
FILE_KEYWORD = "businessinvolvementscreens"    # Keyword that must be in filename (lowercase)
SUPPORTED_EXTENSIONS = [".xls", ".xlsx"]       # Supported Excel file extensions

# =============================================================================
# END CONFIGURATION SECTION
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Build paths from configuration
INPUT_DIR = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BASE_PATH}"
if INPUT_SUBPATH:
    INPUT_DIR = f"{INPUT_DIR}/{INPUT_SUBPATH}"

OUTPUT_DIR = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BASE_PATH}/{OUTPUT_SUBPATH}"

ERROR_DIR = f"{OUTPUT_DIR}/errors"
LOG_DIR = f"{OUTPUT_DIR}/logs"

# Excel Output Configuration
EXCEL_OUTPUT_FILE = f"{OUTPUT_DIR}/unified_company_esg_data.xlsx"

# Initialize Spark Session for distributed processing
spark = SparkSession.builder.appName("S&P_ESG_Processor").getOrCreate()

# Always use Azure Blob Storage for temporary files (works on all cluster types)
TEMP_DIR = f"{OUTPUT_DIR}/temp"
TEMP_LOCATION = "azure"
CLUSTER_TYPE = "Universal (Azure Blob Temp)"

# Ensure temp directory exists in Azure
dbutils.fs.mkdirs(TEMP_DIR)

# Log configuration on startup
logger.info("=== S&P Global ESG Processor Configuration ===")
logger.info(f"Cluster Type: {CLUSTER_TYPE}")
logger.info(f"Temporary Directory: {TEMP_DIR} ({TEMP_LOCATION})")
logger.info(f"Storage Account: {STORAGE_ACCOUNT_NAME}")
logger.info(f"Container: {CONTAINER_NAME}")
logger.info(f"Input Directory: {INPUT_DIR}")
logger.info(f"Output Directory: {OUTPUT_DIR}")
logger.info(f"Max Retries: {MAX_RETRIES}")
logger.info(f"Batch Size: {BATCH_SIZE}")
logger.info(f"File Keyword Filter: {FILE_KEYWORD}")
logger.info("=" * 50)

# Verify Azure temp directory is accessible
logger.info(f"Testing Azure temp directory access: {TEMP_DIR}")
try:
    test_file = f"{TEMP_DIR}/startup_test_{int(time.time())}.txt"
    dbutils.fs.put(test_file, "test", True)
    dbutils.fs.rm(test_file)
    logger.info(f"✓ Azure temp directory {TEMP_DIR} is accessible")
except Exception as e:
    logger.error(f"✗ Azure temp directory {TEMP_DIR} is NOT accessible: {e}")
    logger.error("This will cause file processing errors!")


# def get_safe_local_path(original_path):
#     """Create a safe local path for very long filenames"""
#     filename = original_path.split("/")[-1]
    
#     # Check if filename is too long (leave room for thread ID and temp path)
#     if len(filename) > 180:  # Conservative threshold
#         # Create hash of filename to make it shorter
#         filename_hash = hashlib.md5(filename.encode()).hexdigest()
#         extension = os.path.splitext(filename)[1]
#         safe_filename = f"{filename_hash}{extension}"
#     else:
#         safe_filename = filename
        
#     return f"/tmp/{threading.get_ident()}_{safe_filename}"
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


@functools.lru_cache(maxsize=128)
def detect_currency_in_header(column_name):
    """
    Dynamically detect currency information inside parentheses in column headers.
    Enhanced to detect Swedish and other international currency formats.
    """
    # Look for text inside parentheses
    parentheses_match = re.search(r'\((.*?)\)', column_name)
    if not parentheses_match: return False, None
    parentheses_content = parentheses_match.group(1)
    non_currency_patterns = [r'%', r'actual', r'forecast', r'count', r'total', r'max', r'min', r'expected', r'estimated', r'proportion']
    for pattern in non_currency_patterns:
        if re.search(pattern, parentheses_content.lower()): return False, None
    currency_symbols = r'[$€£¥₹₽₩₺₴₣₦₱₲₪₫₭₸₼₾]'
    if re.search(currency_symbols, parentheses_content): return True, parentheses_content
    currency_code_pattern = r'^([A-Z]{2,3})\s*[MB]?$'
    if re.match(currency_code_pattern, parentheses_content): return True, parentheses_content
    if re.match(r'^[A-Z]{2,3}[\$€£¥₹₽₩₺₴₣₦₱₲₪₫₭₸₼₾]M?$', parentheses_content): return True, parentheses_content
    if re.match(r'^[\$€£¥₹₽₩₺₴₣₦₱₲₪₫₭₸₼₾]\s*[MB]$', parentheses_content): return True, parentheses_content
    if re.match(r'^(SEK|SKR)[mM]$', parentheses_content) or \
       re.match(r'^[Mm](SEK|SKR|kr|Kr)$', parentheses_content) or \
       re.match(r'^M(SEK|SKR|kr|Kr|KR)$', parentheses_content) or \
       re.match(r'^(kr|Kr|KR)M$', parentheses_content) or \
       parentheses_content in ['SEKm', 'SEKt', 'MSEK', 'Mkr', 'MKR', 'TSEK']: return True, parentheses_content
    if re.match(r'^(EUR?|GBP)\s*[mM]$', parentheses_content) or \
       re.match(r'^[mM](EUR?|GBP)$', parentheses_content) or \
       re.match(r'^[kKtT](EUR?|GBP|USD)$', parentheses_content) or \
       parentheses_content in ['EURm', 'EURk', 'GBPm', 'GBPk', 'USDm', 'USDk']: return True, parentheses_content
    if (len(parentheses_content) <= 5 and
        ('M' in parentheses_content or 'B' in parentheses_content or 'K' in parentheses_content) and
        not re.search(r'[a-z]', parentheses_content)): return True, parentheses_content
    return False, None


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
            content = "# S&P Global Business Involvement Screens processed files\n"
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


def process_excel_file(file_path):
    """
    Process a single S&P Excel file and returns extracted data for three categories.
    The core data identification and extraction logic remains the same.
    Instead of writing to Excel files, it returns lists of rows (data + headers).
    """
    try:
        file_name_on_dbfs = file_path.split("/")[-1]
        
        # Read Excel file using the new method
        df = _read_excel_file(file_path, file_name_on_dbfs)
        
        if df is None: 
            return False, None, f"Failed to read Excel file {file_name_on_dbfs}"

        # Create displayed_values for date formatting compatibility
        displayed_values = df.values.tolist()
        json_data = df.values.tolist()
        
        # --- CORE EXTRACTION LOGIC ---
        orig_filename_no_ext = os.path.splitext(file_name_on_dbfs)[0]

        company_name_ext = ""
        company_key_ext = ""
        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]) and isinstance(row[0], str) and "|" in row[0]:
                title_parts = row[0].split('|')
                if len(title_parts) > 0: company_name_ext = title_parts[0].strip()
                break
        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]) and isinstance(row[0], str) and "MI KEY:" in row[0] and "SPCIQ KEY:" in row[0]:
                company_key_ext = row[0].strip()
                if not company_name_ext:
                    key_parts = company_key_ext.split('(')
                    if len(key_parts) > 0: company_name_ext = key_parts[0].strip()
                break
        if not company_name_ext or not company_key_ext:
            company_name_ext = orig_filename_no_ext
            company_key_ext = "Unknown"

        description_ext = ""
        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]) and row[0] == "Company Description":
                if i + 1 < len(json_data) and pd.notna(json_data[i+1][0]): description_ext = json_data[i+1][0]
                break
        
        primary_gics_ext, fiscal_year_ext, analysis_date_ext = "", "", ""
        total_revenue_ext, total_revenue_exposed_ext, revenue_exposed_percent_ext, expansion_exposure_ext = "", "", "", ""
        esg_details_currency_ext = None

        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]):
                val_str = str(row[0])
                if val_str == "Primary GICS Sub-Industry" and len(row) > 1 and pd.notna(row[1]): primary_gics_ext = row[1]
                elif val_str == "Fiscal Year" and len(row) > 1 and pd.notna(row[1]): fiscal_year_ext = displayed_values[i][1] if i < len(displayed_values) and 1 < len(displayed_values[i]) else row[1]
                elif val_str == "Analysis Date" and len(row) > 1 and pd.notna(row[1]): analysis_date_ext = displayed_values[i][1] if i < len(displayed_values) and 1 < len(displayed_values[i]) else row[1]
                elif "Total Revenue" in val_str and "Exposed" not in val_str and len(row) > 1 and pd.notna(row[1]):
                    total_revenue_ext = row[1]
                    has_curr, curr = detect_currency_in_header(val_str)
                    if has_curr and esg_details_currency_ext is None: esg_details_currency_ext = curr
                elif "Total Revenue Exposed" in val_str and len(row) > 1:
                    total_revenue_exposed_ext = 0 if pd.isna(row[1]) else row[1]
                    has_curr, curr = detect_currency_in_header(val_str)
                    if has_curr and esg_details_currency_ext is None: esg_details_currency_ext = curr
                elif val_str == "Revenue Exposed/ Total Revenue (%)" and len(row) > 1: revenue_exposed_percent_ext = 0 if pd.isna(row[1]) else row[1]
                elif val_str == "Expansion Exposure" and len(row) > 1 and pd.notna(row[1]): expansion_exposure_ext = row[1]
        if esg_details_currency_ext is None: esg_details_currency_ext = "$M"

        esg_company_details_headers = ["Name", "Key", "Description", "Primary GICS Sub-Industry", "Fiscal Year", "Analysis Date", "Currency", "Total Revenue", "Total Revenue Exposed", "Revenue Exposed/ Total Revenue (%)", "Expansion Exposure"]
        esg_company_details_row = [company_name_ext, company_key_ext, description_ext, primary_gics_ext, fiscal_year_ext, analysis_date_ext, esg_details_currency_ext, total_revenue_ext, total_revenue_exposed_ext, revenue_exposed_percent_ext, expansion_exposure_ext]
        esg_company_details_data = [esg_company_details_headers, esg_company_details_row] # Header + 1 data row

        # BIS Data Extraction
        bis_currency_ext = None # Detect or default
        # ... your full BIS currency detection ...
        if bis_currency_ext is None: bis_currency_ext = esg_details_currency_ext
        
        bis_headers_list = ["Name", "Key", "Business Involvement Screen Group", "Currency", "Business Involvement Screen Revenue", "Business Involvement Screen Revenue (%)", "Total Revenue", "Exclusionary Screen Maximum Ownership (%)", "Exclusionary Screen Ownership Count (actual)", "Exclusionary Screen Max Ownership Count (actual)"]
        business_involvement_screen_data = [bis_headers_list]
        
        # BIS group finding logic
        found_bis_header_flag_ext = False
        bis_groups_found_ext = [] # This will store all found BIS groups
        
        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]):
                current_cell_str = str(row[0])
                if current_cell_str == "Business Involvement Screen Group" and len(row) > 1 and pd.notna(row[1]): 
                    found_bis_header_flag_ext = True; 
                    continue
                elif current_cell_str == "Business Involvement Screen Groups" and i+2 < len(json_data) and pd.notna(json_data[i+2][0]) and json_data[i+2][0] == "Business Involvement Screen Group": 
                    found_bis_header_flag_ext = True; 
                    continue
                
                if found_bis_header_flag_ext and current_cell_str != "":
                    is_data_row_ext = False
                    if len(row) > 3 and pd.notna(row[3]):
                        try:
                            row_3_val = float(row[3]) if isinstance(row[3], (int, float)) else row[3]
                            tr_val = float(total_revenue_ext) if isinstance(total_revenue_ext, (int, float)) else total_revenue_ext
                            if row_3_val == tr_val or (isinstance(row_3_val, float) and isinstance(tr_val, float) and abs(row_3_val - tr_val) < 0.01): 
                                is_data_row_ext = True
                        except (ValueError, TypeError):
                             if str(row[3]) == str(total_revenue_ext): 
                                 is_data_row_ext = True
                    
                    if is_data_row_ext:
                        bis_groups_found_ext.append({
                            "group": row[0], 
                            "revenue": row[1] if len(row) > 1 and pd.notna(row[1]) else "-", 
                            "revenue_percent": row[2] if len(row) > 2 and pd.notna(row[2]) else "-", 
                            "total_revenue": row[3] if len(row) > 3 and pd.notna(row[3]) else total_revenue_ext, 
                            "max_ownership": row[4] if len(row) > 4 and pd.notna(row[4]) else "-", 
                            "ownership_count": row[5] if len(row) > 5 and pd.notna(row[5]) else 0, 
                            "max_ownership_count": row[6] if len(row) > 6 and pd.notna(row[6]) else "-"
                        })
                    elif current_cell_str == "Involvement Type": 
                        break

        # Log BIS data for debugging
        logger.debug(f"BIS groups found in {file_name_on_dbfs}: {len(bis_groups_found_ext)}")
        if bis_groups_found_ext:
            logger.debug(f"First BIS group: {bis_groups_found_ext[0]}")
        else:
            logger.debug(f"No BIS groups found in {file_name_on_dbfs}, using fallback")
            
        if not bis_groups_found_ext: # Fallback
            fallback_categories_ext = {
                "Consumer Products & Services": {"max_ownership_count": 25}, 
                "Defense & Weapons": {"max_ownership_count": 25}, 
                "Energy & Fossil Fuels": {"max_ownership_count": 14}, 
                "Healthcare": {"max_ownership_count": 10}
            }
            
            for category, details in fallback_categories_ext.items():
                if any(str(r[0]) == category for r_idx, r in enumerate(json_data) if r_idx < len(json_data) and pd.notna(r[0])):
                    bis_groups_found_ext.append({
                        "group": category, 
                        "revenue": "-", 
                        "revenue_percent": "-", 
                        "total_revenue": total_revenue_ext, 
                        "max_ownership": "-", 
                        "ownership_count": 0, 
                        "max_ownership_count": details["max_ownership_count"]
                    })
        
        # Add BIS data rows
        for bis_ext in bis_groups_found_ext:
             business_involvement_screen_data.append([
                 company_name_ext, 
                 company_key_ext, 
                 bis_ext["group"], 
                 bis_currency_ext, 
                 bis_ext["revenue"], 
                 bis_ext["revenue_percent"], 
                 bis_ext["total_revenue"], 
                 bis_ext["max_ownership"], 
                 bis_ext["ownership_count"], 
                 bis_ext["max_ownership_count"]
             ])

        # PS Data Extraction
        ps_currency_ext = None # Detect or default
        if ps_currency_ext is None: ps_currency_ext = esg_details_currency_ext

        ps_headers_list = ["Name", "Key", "Involvement Type", "Subcategory", "Currency", "Revenue", "Revenue Exposed/ Total Revenue (%)", "Calculation Method", "Revenue Range", "Ownership Involvement (%)", "Ownership Range", "Source Document", "All Sources"]
        esg_products_services_data = [ps_headers_list]

        # PS entry finding logic
        found_ps_header_flag_ext = False
        current_ps_category_ext = ""
        ps_entries_found_ext = [] # This will store all found PS entries
        
        for i, row in enumerate(json_data):
            if i < len(json_data) and pd.notna(row[0]):
                current_cell_str = str(row[0])
                if current_cell_str == "Involvement Type" and len(row) > 2 and pd.notna(row[1]) and pd.notna(row[2]): 
                    found_ps_header_flag_ext = True; 
                    continue
                
                if found_ps_header_flag_ext:
                    if current_cell_str.startswith("        "):
                        if current_ps_category_ext:
                            ps_entries_found_ext.append({
                                "type": current_ps_category_ext, 
                                "subtype": current_cell_str.strip(), 
                                "revenue": row[1] if len(row) > 1 and pd.notna(row[1]) else "-", 
                                "revenue_percent": row[2] if len(row) > 2 and pd.notna(row[2]) else "-", 
                                "calculation_method": row[3] if len(row) > 3 and pd.notna(row[3]) else "-", 
                                "revenue_range": row[4] if len(row) > 4 and pd.notna(row[4]) else "No Involvement", 
                                "ownership_involvement": row[5] if len(row) > 5 and pd.notna(row[5]) else "-", 
                                "ownership_range": row[6] if len(row) > 6 and pd.notna(row[6]) else "No Involvement", 
                                "source_document": row[7] if len(row) > 7 and pd.notna(row[7]) else "-", 
                                "all_sources": row[8] if len(row) > 8 and pd.notna(row[8]) else "-"
                            })
                    elif current_cell_str == "Business Involvement Screen Group" or (isinstance(current_cell_str, str) and "Further information is locked" in current_cell_str) or (current_cell_str in ["Defense & Weapons", "Energy & Fossil Fuels", "Healthcare"] and not ps_entries_found_ext):
                        if not (isinstance(current_cell_str, str) and "Further information is locked" in current_cell_str): 
                            break
                    elif current_cell_str != "":
                        current_ps_category_ext = current_cell_str
                        ps_entries_found_ext.append({
                            "type": current_ps_category_ext, 
                            "subtype": "", 
                            "revenue": row[1] if len(row) > 1 and pd.notna(row[1]) else "-", 
                            "revenue_percent": row[2] if len(row) > 2 and pd.notna(row[2]) else "-", 
                            "calculation_method": row[3] if len(row) > 3 and pd.notna(row[3]) else "-", 
                            "revenue_range": row[4] if len(row) > 4 and pd.notna(row[4]) else "No Involvement", 
                            "ownership_involvement": row[5] if len(row) > 5 and pd.notna(row[5]) else "-", 
                            "ownership_range": row[6] if len(row) > 6 and pd.notna(row[6]) else "No Involvement", 
                            "source_document": row[7] if len(row) > 7 and pd.notna(row[7]) else "-", 
                            "all_sources": row[8] if len(row) > 8 and pd.notna(row[8]) else "-"
                        })
                        
        if not ps_entries_found_ext: # Fallback
            default_ps_categories_ext = {
                "Tobacco": ["Retail and Distribution", "Related Products and Services", "Production"], 
                "Predatory Lending": ["Supporting Products and Services", "Operations"], 
                "Pesticides": ["Production", "Retail"], 
                "Palm Oil": ["Growers", "Processors and Traders"], 
                "Genetically Modified Organisms": ["Development", "Growth"], 
                "Gambling": ["Supporting Products and Services", "Operations", "Specialized Equipment"], 
                "Fur and Specialty Leather": ["Retail and Distribution", "Production"], 
                "Cannabis": ["Retail (Medical)", "Retail (Recreational)", "Wholesale (Medical)", "Wholesale (Recreational)"], 
                "Animal Welfare": ["Household, Cosmetics and Personal Care Product Animal Testing", "Food and Beverage Animal Testing", "Chemicals Animal Testing", "Animal Testers and Breeders", "Animal Testing Suspected"], 
                "Alcohol": ["Retail and Distribution", "Related Products and Services", "Production"], 
                "Adult Entertainment": ["Retail and Distribution", "Production"]
            }
            
            for category, subcategories in default_ps_categories_ext.items():
                ps_entries_found_ext.append({
                    "type": category, 
                    "subtype": "", 
                    "revenue": "-", 
                    "revenue_percent": "-", 
                    "calculation_method": "-", 
                    "revenue_range": "No Involvement", 
                    "ownership_involvement": "-", 
                    "ownership_range": "No Involvement", 
                    "source_document": "-", 
                    "all_sources": "-"
                })
                
                for subcat in subcategories:
                    ps_entries_found_ext.append({
                        "type": category, 
                        "subtype": subcat, 
                        "revenue": "-", 
                        "revenue_percent": "-", 
                        "calculation_method": "-", 
                        "revenue_range": "No Involvement", 
                        "ownership_involvement": "-", 
                        "ownership_range": "No Involvement", 
                        "source_document": "-", 
                        "all_sources": "-"
                    })
                    
        # Add PS data rows
        for ps_ext in ps_entries_found_ext:
             esg_products_services_data.append([
                 company_name_ext, 
                 company_key_ext, 
                 ps_ext["type"], 
                 ps_ext["subtype"], 
                 ps_currency_ext, 
                 ps_ext["revenue"], 
                 ps_ext["revenue_percent"], 
                 ps_ext["calculation_method"], 
                 ps_ext["revenue_range"], 
                 ps_ext["ownership_involvement"], 
                 ps_ext["ownership_range"], 
                 ps_ext["source_document"], 
                 ps_ext["all_sources"]
             ])
             
        logger.debug(f"Data extracted for {file_name_on_dbfs}")
        returned_data = {
            "company_details": esg_company_details_data,  # [[headers], [row1_values]]
            "bis_data": business_involvement_screen_data, # [[headers], [row1_values], [row2_values]...]
            "ps_data": esg_products_services_data         # [[headers], [row1_values], [row2_values]...]
        }
        
        # Log data statistics for debugging
        logger.debug(f"Company details rows: {len(esg_company_details_data)-1}")
        logger.debug(f"BIS rows: {len(business_involvement_screen_data)-1}")
        logger.debug(f"PS rows: {len(esg_products_services_data)-1}")
        
        return True, returned_data, "" # success, data_dict, no_error_message

    except Exception as e:
        error_msg = f"Error in process_excel_file for {file_path.split('/')[-1]}: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg # success=False, no data, error_message


def process_excel_file_wrapper(file_path_arg, error_dir_arg, max_retries_arg):
    """Wrapper for process_excel_file to be used with ThreadPoolExecutor."""
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
            
            success, data_payload, error_message = process_excel_file(file_path_arg)
            
            if success:
                logger.info(f"Thread {current_thread_name} successfully extracted data from: {file_name}")
                return True, file_path_arg, data_payload, None  # success, filepath, data, no error
            else:
                logger.warning(f"Thread {current_thread_name} attempt {attempt + 1} failed for {file_name}: {error_message}")
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


def process_files_with_spark(files_to_process, max_retries):
    """Process files using Spark RDD for distributed processing across the cluster"""
    logger.info("🚀 Using Spark distributed processing across cluster nodes")
    
    def spark_process_file(file_path):
        """Function to be executed on Spark executors"""
        import os
        import time
        import random
        
        file_name = file_path.split("/")[-1]
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = random.uniform(0.5, 1.5) * (attempt + 1)
                    time.sleep(wait_time)
                
                success, data_payload, error_message = process_excel_file(file_path)
                
                if success:
                    return (True, file_path, data_payload, None)
                else:
                    if attempt == max_retries - 1:  # Last attempt
                        return (False, file_path, None, error_message)
                    
            except Exception as e:
                error_message = f"Spark executor exception for {file_name}: {str(e)}"
                if attempt == max_retries - 1:  # Last attempt
                    return (False, file_path, None, error_message)
        
        return (False, file_path, None, "Max retries exceeded")
    
    # Create RDD and process files across cluster
    files_rdd = spark.sparkContext.parallelize(files_to_process, numSlices=len(files_to_process) // 10)  # 10 files per partition
    results_rdd = files_rdd.map(spark_process_file)
    
    # Collect results
    results = results_rdd.collect()
    
    return results
    

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


def create_unified_excel_file(company_details_data, bis_data, ps_data, output_file_path):
    """
    Create a unified Excel file with comprehensive company data in a single sheet.
    Also creates separate Excel files for individual data types.
    """
    try:
        # Convert lists to DataFrames
        if company_details_data and len(company_details_data) > 1:
            cd_df = pd.DataFrame(company_details_data[1:], columns=company_details_data[0])
        else:
            cd_df = pd.DataFrame()
            
        if bis_data and len(bis_data) > 1:
            bis_df = pd.DataFrame(bis_data[1:], columns=bis_data[0])
        else:
            bis_df = pd.DataFrame()
            
        if ps_data and len(ps_data) > 1:
            ps_df = pd.DataFrame(ps_data[1:], columns=ps_data[0])
        else:
            ps_df = pd.DataFrame()
        
        # Get safe paths for Excel file creation (always use Azure temp)
        timestamp = int(time.time())
        local_excel_path = f"/Workspace/tmp/unified_company_esg_data_{timestamp}.xlsx"
        azure_temp_path = f"{TEMP_DIR}/unified_company_esg_data_{timestamp}.xlsx"
        os.makedirs("/Workspace/tmp", exist_ok=True)
        
        # === CREATE MAIN UNIFIED EXCEL FILE (SINGLE SHEET) ===
        if not cd_df.empty:
            # Group BIS data by key
            bis_grouped = {}
            if not bis_df.empty and 'key' in bis_df.columns:
                for _, row in bis_df.iterrows():
                    key = row['key']
                    if key not in bis_grouped:
                        bis_grouped[key] = []
                    bis_grouped[key].append({
                        'group': row.get('business_involvement_screen_group', ''),
                        'revenue': row.get('business_involvement_screen_revenue', ''),
                        'revenue_pct': row.get('business_involvement_screen_revenue_pct', ''),
                        'max_ownership': row.get('exclusionary_screen_maximum_ownership_pct', ''),
                        'ownership_count': row.get('exclusionary_screen_ownership_count_actual', ''),
                        'max_ownership_count': row.get('exclusionary_screen_max_ownership_count_actual', ''),
                        'bis_currency': row.get('currency', '')
                    })
            
            # Group PS data by key
            ps_grouped = {}
            if not ps_df.empty and 'key' in ps_df.columns:
                for _, row in ps_df.iterrows():
                    key = row['key']
                    if key not in ps_grouped:
                        ps_grouped[key] = []
                    ps_grouped[key].append({
                        'involvement_type': row.get('involvement_type', ''),
                        'subcategory': row.get('subcategory', ''),
                        'revenue': row.get('revenue', ''),
                        'revenue_pct': row.get('revenue_exposed_total_revenue_pct', ''),
                        'calculation_method': row.get('calculation_method', ''),
                        'revenue_range': row.get('revenue_range', ''),
                        'ownership_involvement': row.get('ownership_involvement_pct', ''),
                        'ownership_range': row.get('ownership_range', ''),
                        'source_document': row.get('source_document', ''),
                        'all_sources': row.get('all_sources', ''),
                        'ps_currency': row.get('currency', '')
                    })
            
            # Create comprehensive unified records
            unified_data = []
            for _, cd_row in cd_df.iterrows():
                company_key = cd_row.get('key', '')
                
                # Get BIS and PS data for this company
                bis_records = bis_grouped.get(company_key, [])
                ps_records = ps_grouped.get(company_key, [])
                
                # Create detailed BIS information
                bis_groups = '; '.join([rec['group'] for rec in bis_records if rec['group']])
                bis_revenues = '; '.join([str(rec['revenue']) for rec in bis_records if rec['revenue'] and str(rec['revenue']) != '-'])
                bis_revenue_pcts = '; '.join([str(rec['revenue_pct']) for rec in bis_records if rec['revenue_pct'] and str(rec['revenue_pct']) != '-'])
                bis_max_ownerships = '; '.join([str(rec['max_ownership']) for rec in bis_records if rec['max_ownership'] and str(rec['max_ownership']) != '-'])
                
                # Create detailed PS information
                ps_types = '; '.join([rec['involvement_type'] for rec in ps_records if rec['involvement_type']])
                ps_subcategories = '; '.join([rec['subcategory'] for rec in ps_records if rec['subcategory']])
                ps_revenues = '; '.join([str(rec['revenue']) for rec in ps_records if rec['revenue'] and str(rec['revenue']) != '-'])
                ps_revenue_pcts = '; '.join([str(rec['revenue_pct']) for rec in ps_records if rec['revenue_pct'] and str(rec['revenue_pct']) != '-'])
                ps_revenue_ranges = '; '.join([rec['revenue_range'] for rec in ps_records if rec['revenue_range'] and rec['revenue_range'] != 'No Involvement'])
                ps_ownership_involvements = '; '.join([str(rec['ownership_involvement']) for rec in ps_records if rec['ownership_involvement'] and str(rec['ownership_involvement']) != '-'])
                ps_ownership_ranges = '; '.join([rec['ownership_range'] for rec in ps_records if rec['ownership_range'] and rec['ownership_range'] != 'No Involvement'])
                
                unified_record = {
                    # Company Details
                    'Company_Name': cd_row.get('name', ''),
                    'Company_Key': company_key,
                    'Description': cd_row.get('description', ''),
                    'Primary_GICS_Sub_Industry': cd_row.get('primary_gics_subindustry', ''),
                    'Fiscal_Year': cd_row.get('fiscal_year', ''),
                    'Analysis_Date': cd_row.get('analysis_date', ''),
                    'Currency': cd_row.get('currency', ''),
                    'Total_Revenue': cd_row.get('total_revenue', ''),
                    'Total_Revenue_Exposed': cd_row.get('total_revenue_exposed', ''),
                    'Revenue_Exposed_Percent': cd_row.get('revenue_exposed_total_revenue_pct', ''),
                    'Expansion_Exposure': cd_row.get('expansion_exposure', ''),
                    
                    # BIS Summary
                    'BIS_Count': len(bis_records),
                    'BIS_Groups': bis_groups,
                    'BIS_Revenues': bis_revenues,
                    'BIS_Revenue_Percentages': bis_revenue_pcts,
                    'BIS_Max_Ownerships': bis_max_ownerships,
                    
                    # PS Summary
                    'PS_Count': len(ps_records),
                    'PS_Involvement_Types': ps_types,
                    'PS_Subcategories': ps_subcategories,
                    'PS_Revenues': ps_revenues,
                    'PS_Revenue_Percentages': ps_revenue_pcts,
                    'PS_Revenue_Ranges': ps_revenue_ranges,
                    'PS_Ownership_Involvements': ps_ownership_involvements,
                    'PS_Ownership_Ranges': ps_ownership_ranges
                }
                unified_data.append(unified_record)
            
            # Create main Excel file with single unified sheet
            with pd.ExcelWriter(local_excel_path, engine='openpyxl') as writer:
                if unified_data:
                    unified_df = pd.DataFrame(unified_data)
                    unified_df.to_excel(writer, sheet_name='Unified_Company_ESG_Data', index=False)
                    logger.info(f"Created unified Excel with {len(unified_df)} company records")
        
        # Copy main file to final output location via Azure temp
        dbutils.fs.cp(f"file:{local_excel_path}", azure_temp_path)
        dbutils.fs.cp(azure_temp_path, output_file_path)
        # Clean up both local and Azure temp
        os.remove(local_excel_path)
        dbutils.fs.rm(azure_temp_path)
        
        logger.info(f"Successfully created main unified Excel file: {output_file_path}")
        
        # === CREATE SEPARATE EXCEL FILES FOR INDIVIDUAL DATA TYPES ===
        base_path = output_file_path.rsplit('/', 1)[0]
        timestamp = output_file_path.split('_')[-1].replace('.xlsx', '')
        
        def create_and_copy_excel(df, sheet_name, file_prefix, output_path):
            """Helper function to create Excel files via Azure temp"""
            local_path = f"/Workspace/tmp/{file_prefix}_{int(time.time())}.xlsx"
            azure_temp_path = f"{TEMP_DIR}/{file_prefix}_{int(time.time())}.xlsx"
            
            with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            dbutils.fs.cp(f"file:{local_path}", azure_temp_path)
            dbutils.fs.cp(azure_temp_path, output_path)
            os.remove(local_path)
            dbutils.fs.rm(azure_temp_path)
        
        # Create separate Company Details Excel
        if not cd_df.empty:
            cd_output_path = f"{base_path}/company_details_{timestamp}.xlsx"
            create_and_copy_excel(cd_df, 'Company_Details', 'company_details', cd_output_path)
            logger.info(f"Created separate Company Details Excel: {cd_output_path}")
        
        # Create separate BIS Excel
        if not bis_df.empty:
            bis_output_path = f"{base_path}/business_involvement_screens_{timestamp}.xlsx"
            create_and_copy_excel(bis_df, 'Business_Involvement_Screens', 'business_involvement', bis_output_path)
            logger.info(f"Created separate BIS Excel: {bis_output_path}")
        
        # Create separate PS Excel
        if not ps_df.empty:
            ps_output_path = f"{base_path}/products_services_{timestamp}.xlsx"
            create_and_copy_excel(ps_df, 'Products_Services', 'products_services', ps_output_path)
            logger.info(f"Created separate PS Excel: {ps_output_path}")
        
        return True, output_file_path
        
    except Exception as e:
        error_msg = f"Error creating unified Excel file: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, error_msg




def main():
    tracking_file = setup_directories()
    processed_files_set_on_start = get_processed_files(tracking_file)
    newly_processed_in_this_run = set()
    
    # Get list of files to process
    file_list_from_dbfs = dbutils.fs.ls(INPUT_DIR)
    files_to_process_this_run = []
    
    for file_info in file_list_from_dbfs:
        if file_info.isDir(): 
            logger.debug(f"Skipping directory: {file_info.path}")
            continue
        
        file_path = file_info.path
        file_name = file_info.name 
        fn_lower = file_name.lower()
        is_excel = any(fn_lower.endswith(ext) for ext in SUPPORTED_EXTENSIONS)
        contains_keyword = FILE_KEYWORD in fn_lower 
        
        if is_excel and contains_keyword:
            if file_path not in processed_files_set_on_start:
                files_to_process_this_run.append(file_path)
            else:
                logger.info(f"Skipping already processed file (from tracking): {file_name}")
        else:
            logger.debug(f"Skipping file (name/ext mismatch or not relevant): {file_path}")
            
    if not files_to_process_this_run:
        logger.info("No new Excel files matching criteria found to process in this run.")
        return
    
    logger.info(f"Found {len(files_to_process_this_run)} new Excel files to process in this run.")
    
    # Get cluster information for distributed processing
    try:
        # Get total cores across the cluster
        total_cores = int(spark.conf.get("spark.executor.cores", "2")) * int(spark.conf.get("spark.executor.instances", "2"))
        driver_cores = int(spark.conf.get("spark.driver.cores", "2"))
        total_cluster_cores = total_cores + driver_cores
        
        # Use more parallelism for cluster processing
        max_workers = min(MAX_WORKERS, max(8, total_cluster_cores * 2)) # More aggressive parallelism
        
        logger.info(f"Cluster Info - Driver cores: {driver_cores}, Executor cores: {total_cores}, Total: {total_cluster_cores}")
        logger.info(f"Using up to {max_workers} worker threads for parallel processing.")
        
        # Option to use Spark RDD for distributed processing
        USE_SPARK_DISTRIBUTION = len(files_to_process_this_run) > 100  # Use Spark for large datasets
        
        if USE_SPARK_DISTRIBUTION:
            logger.info("🚀 Large dataset detected - enabling Spark distributed processing")
        else:
            logger.info("📝 Small dataset - using multi-threading on driver")
            
    except Exception as e:
        logger.warning(f"Could not get cluster info: {e}. Using default threading.")
        max_workers = min(MAX_WORKERS, max(1, os.cpu_count() or 4))
        USE_SPARK_DISTRIBUTION = False
    
    # Global variables to store the (sanitized) headers once captured
    company_details_headers = None
    bis_headers = None
    ps_headers = None
    headers_captured_flag = False # Flag to indicate if all three header sets have been captured
    
    # Global data collectors for all processed files
    all_company_details_rows = []
    all_bis_rows = []
    all_ps_rows = []
    
    # Process in batches
    batch_size = BATCH_SIZE
    total_batches = (len(files_to_process_this_run) + batch_size - 1) // batch_size
    
    total_successful_files = 0
    total_failed_files = []
    
    # Process each batch
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(files_to_process_this_run))
        current_batch = files_to_process_this_run[start_idx:end_idx]
        
        logger.info(f"Processing batch {batch_idx+1}/{total_batches}: files {start_idx+1} to {end_idx} ({len(current_batch)} files)")
        
        # Reset data collectors for this batch
        batch_company_details_rows = []
        batch_bis_rows = []
        batch_ps_rows = []
        
        # Process this batch
        successful_batch_files = 0
        failed_batch_files = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="S&P_FileWorker") as executor:
            future_to_filepath = {
                executor.submit(process_excel_file_wrapper, fp, ERROR_DIR, MAX_RETRIES): fp
                for fp in current_batch
            }
            
            for future in concurrent.futures.as_completed(future_to_filepath):
                original_filepath = future_to_filepath[future]
                original_filename = original_filepath.split('/')[-1]
                try:
                    was_success, processed_fp, returned_data_dict, error_msg = future.result()
                    
                    if was_success and returned_data_dict:
                        newly_processed_in_this_run.add(processed_fp)
                        successful_batch_files += 1
                        
                        # --- Step 1: Attempt to capture and sanitize headers if not already done ---
                        if not headers_captured_flag:
                            temp_cd_headers, temp_bis_headers, temp_ps_headers = None, None, None
                            
                            # Check if data for each section exists and has at least a header row
                            if "company_details" in returned_data_dict and returned_data_dict["company_details"] and len(returned_data_dict["company_details"]) > 0:
                                temp_cd_headers = [sanitize_column_name(h) for h in returned_data_dict["company_details"][0]]
                            if "bis_data" in returned_data_dict and returned_data_dict["bis_data"] and len(returned_data_dict["bis_data"]) > 0:
                                temp_bis_headers = [sanitize_column_name(h) for h in returned_data_dict["bis_data"][0]]
                            if "ps_data" in returned_data_dict and returned_data_dict["ps_data"] and len(returned_data_dict["ps_data"]) > 0:
                                temp_ps_headers = [sanitize_column_name(h) for h in returned_data_dict["ps_data"][0]]
                            
                            if temp_cd_headers and temp_bis_headers and temp_ps_headers:
                                company_details_headers = temp_cd_headers
                                bis_headers = temp_bis_headers
                                ps_headers = temp_ps_headers
                                headers_captured_flag = True
                                logger.info(f"Captured and sanitized headers from {original_filename}. Examples: CD: {company_details_headers[0]}, BIS: {bis_headers[0]}, PS: {ps_headers[0]}")
                            else:
                                logger.warning(f"File {original_filename} processed successfully but did not yield all three header sets. Headers still not fully captured.")
                        
                        # --- Step 2: Append data rows ONLY IF headers have been successfully captured ---
                        if headers_captured_flag:
                            if "company_details" in returned_data_dict and returned_data_dict["company_details"] and len(returned_data_dict["company_details"]) > 1:
                                batch_company_details_rows.extend(returned_data_dict["company_details"][1:])
                                all_company_details_rows.extend(returned_data_dict["company_details"][1:])
                            
                            if "bis_data" in returned_data_dict and returned_data_dict["bis_data"] and len(returned_data_dict["bis_data"]) > 1:
                                batch_bis_rows.extend(returned_data_dict["bis_data"][1:])
                                all_bis_rows.extend(returned_data_dict["bis_data"][1:])

                            if "ps_data" in returned_data_dict and returned_data_dict["ps_data"] and len(returned_data_dict["ps_data"]) > 1:
                                batch_ps_rows.extend(returned_data_dict["ps_data"][1:])
                                all_ps_rows.extend(returned_data_dict["ps_data"][1:])
                        elif was_success: # was_success is true, data was returned, but headers_captured_flag is still false from this file
                            logger.info(f"Data was successfully extracted from {original_filename}, but global headers are not yet fully established (some header sections might have been missing from this file). This file's data is currently being skipped for aggregation. Ensure a subsequent file provides all header types.")

                    elif not was_success : # Explicitly check for failure reported by wrapper
                        failed_batch_files.append((original_filename, error_msg))
                    # If was_success is True but returned_data_dict is None (should not happen with current wrapper logic if success is True)
                    elif returned_data_dict is None:
                         logger.warning(f"File {original_filename} reported success but returned no data dict. Skipping aggregation for this file.")


                except Exception as exc:
                    logger.error(f"File {original_filename} generated an unexpected exception in main processing loop: {exc}")
                    logger.debug(traceback.format_exc()) # For detailed stack trace
                    failed_batch_files.append((original_filename, str(exc)))
                    try: 
                        dbutils.fs.cp(original_filepath, f"{ERROR_DIR}/{original_filename}", recurse=False)
                        logger.info(f"Copied file {original_filename} (due to main loop exception) to {ERROR_DIR}")
                    except Exception as copy_exc:
                        logger.error(f"Could not copy {original_filename} to error dir after main loop exception: {copy_exc}")
        
        # Diagnostic info for this batch
        logger.info(f"BATCH {batch_idx+1} DIAGNOSIS - BIS rows in this batch: {len(batch_bis_rows)}")
        logger.info(f"BATCH {batch_idx+1} DIAGNOSIS - Sample of batch_bis_rows: {batch_bis_rows[:2] if batch_bis_rows else 'EMPTY'}")
        
       
        # Log batch processing completion
        logger.info(f"BATCH {batch_idx+1} completed: {successful_batch_files} files processed successfully")
        logger.info(f"BATCH {batch_idx+1} data collected - CD: {len(batch_company_details_rows)}, BIS: {len(batch_bis_rows)}, PS: {len(batch_ps_rows)} rows")
        
        # Update totals
        total_successful_files += successful_batch_files
        total_failed_files.extend(failed_batch_files)
        
        # Force cleanup between batches
        # gc.collect()
        
        logger.info(f"Completed batch {batch_idx+1}/{total_batches}: {successful_batch_files} success, {len(failed_batch_files)} failures")
    
    # Generate unified Excel file after all batches are processed
    if headers_captured_flag and (all_company_details_rows or all_bis_rows or all_ps_rows):
        logger.info("=== Generating Unified Excel File ===")
        logger.info(f"Total data collected - Company Details: {len(all_company_details_rows)}, BIS: {len(all_bis_rows)}, PS: {len(all_ps_rows)} rows")
        
        # Prepare data for Excel generation
        company_details_data = [company_details_headers] + all_company_details_rows if company_details_headers and all_company_details_rows else []
        bis_data = [bis_headers] + all_bis_rows if bis_headers and all_bis_rows else []
        ps_data = [ps_headers] + all_ps_rows if ps_headers and all_ps_rows else []
        
        # Generate timestamped Excel filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_output_path = f"{OUTPUT_DIR}/unified_company_esg_data_{timestamp}.xlsx"
        
        success, result = create_unified_excel_file(
            company_details_data, 
            bis_data, 
            ps_data, 
            excel_output_path
        )
        
        if success:
            logger.info(f"✓ Successfully generated main unified Excel file: {excel_output_path}")
            logger.info(f"✓ Also created separate Excel files for individual data types")
            logger.info(f"Excel files contain data from {total_successful_files} processed files")
        else:
            logger.error(f"✗ Failed to generate Excel files: {result}")
    else:
        logger.warning("No data available for Excel generation or headers not captured")
    
    # Update tracking file with all successfully processed files
    if newly_processed_in_this_run:
        final_processed_set = processed_files_set_on_start.union(newly_processed_in_this_run)
        logger.info(f"Updating tracking file. Total unique processed files: {len(final_processed_set)}")
        tracking_content = "# S&P Global Business Involvement Screens processed files\n# Format: absolute_path_to_file\n"
        for f_path in sorted(list(final_processed_set)): 
            tracking_content += f"{f_path}\n"
        try:
            dbutils.fs.put(tracking_file, tracking_content, True)
            logger.info(f"Successfully updated tracking file: {tracking_file}")
        except Exception as e:
            logger.error(f"CRITICAL: Error updating tracking file {tracking_file} at the end: {str(e)}")

    # Calculate final file counts
    total_files_found = len(files_to_process_this_run)
    total_files_previously_processed = len(processed_files_set_on_start)
    total_files_newly_processed = len(newly_processed_in_this_run)
    total_files_now_processed = len(processed_files_set_on_start.union(newly_processed_in_this_run))
    
    # Final comprehensive summary
    logger.info("=" * 80)
    logger.info("                    FINAL PROCESSING SUMMARY")
    logger.info("=" * 80)
    
    # File Processing Statistics
    logger.info("📊 FILE PROCESSING STATISTICS:")
    logger.info(f"   • Total files found in this run: {total_files_found}")
    logger.info(f"   • Files already processed (skipped): {total_files_previously_processed}")
    logger.info(f"   • Files newly processed in this run: {total_files_newly_processed}")
    logger.info(f"   • Files successfully processed: {total_successful_files}")
    logger.info(f"   • Files failed in this run: {len(total_failed_files)}")
    logger.info(f"   • Total files ever processed: {total_files_now_processed}")
    
    # Data Extraction Statistics
    if headers_captured_flag:
        logger.info("")
        logger.info("📈 DATA EXTRACTION STATISTICS:")
        logger.info(f"   • Company Detail records: {len(all_company_details_rows):,}")
        logger.info(f"   • Business Involvement Screen records: {len(all_bis_rows):,}")
        logger.info(f"   • Products & Services records: {len(all_ps_rows):,}")
        logger.info(f"   • Total data records processed: {len(all_company_details_rows) + len(all_bis_rows) + len(all_ps_rows):,}")
    
    # Configuration Summary
    logger.info("")
    logger.info("⚙️  CONFIGURATION USED:")
    logger.info(f"   • Storage Account: {STORAGE_ACCOUNT_NAME}")
    logger.info(f"   • Container: {CONTAINER_NAME}")
    logger.info(f"   • Input Path: {INPUT_DIR}")
    logger.info(f"   • Output Path: {OUTPUT_DIR}")
    logger.info(f"   • Batch Size: {BATCH_SIZE}")
    logger.info(f"   • Max Workers: {max_workers}")
    
    # Failed Files Details
    if total_failed_files:
        logger.info("")
        logger.info("❌ FAILED FILES DETAILS:")
        for f_name, err_msg in total_failed_files:
            logger.info(f"   • {f_name}: {err_msg}")
    
    # Excel Output Summary
    if headers_captured_flag:
        logger.info("")
        logger.info("📁 EXCEL OUTPUT FILES GENERATED:")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"   • Main unified file: unified_company_esg_data_{timestamp}.xlsx")
        logger.info(f"   • Company details: company_details_{timestamp}.xlsx")
        logger.info(f"   • Business involvement: business_involvement_screens_{timestamp}.xlsx")
        logger.info(f"   • Products services: products_services_{timestamp}.xlsx")
        logger.info(f"   • All files saved to: {OUTPUT_DIR}")
    
    # Success/Failure Status
    logger.info("")
    if total_successful_files > 0:
        logger.info("✅ PROCESSING COMPLETED SUCCESSFULLY!")
        if len(total_failed_files) > 0:
            logger.info(f"⚠️  Note: {len(total_failed_files)} files failed - check error directory")
    else:
        logger.info("❌ NO FILES WERE SUCCESSFULLY PROCESSED")
    
    logger.info("=" * 80)
    logger.info("                    END OF EXECUTION")
    logger.info("=" * 80)

# COMMAND ----------

if __name__ == "__main__":
    main()