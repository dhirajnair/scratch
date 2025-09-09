# Excel Processor for Databricks
# This script processes Excel files containing customer and supplier data and extracts them into CSV files

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import pandas as pd
import glob
from openpyxl.utils.exceptions import InvalidFileException
import datetime
import traceback
import csv
import logging
from io import StringIO, BytesIO
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import openpyxl
import re
from azure.storage.blob import BlobServiceClient
import concurrent.futures
import threading
import time
import random
import uuid
from collections import defaultdict
import io

# Initialize Spark session
spark = SparkSession.builder.appName("ExcelDataProcessor").getOrCreate()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

CONNECTION_STRING = config["connection_string"]
STORAGE_ACCOUNT_NAME = config["storage_account"]  # Replace with your storage account name
CONTAINER_NAME = "raw-data"              # Replace with your container name

# Path Configuration
BASE_PATH = "2.SupplyChain/SNP/30-April-2025/Suppliers"           # Base path within container
INPUT_SUBPATH = ""                   # Subpath for input files
OUTPUT_SUBPATH = "output"        # Subpath for output files

# Processing Configuration
debug_mode = True                              # Enable debug mode for better error logging

# Build paths from configuration
INPUT_DIR = "abfss://" + CONTAINER_NAME + "@" + STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net/" + BASE_PATH
if INPUT_SUBPATH:
    INPUT_DIR = INPUT_DIR + "/" + INPUT_SUBPATH

OUTPUT_DIR = "abfss://" + CONTAINER_NAME + "@" + STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net/" + BASE_PATH + "/" + OUTPUT_SUBPATH
LOG_DIR = OUTPUT_DIR + "/logs"

# Create timestamp for this processing run
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Check if directories exist and create if needed
def ensure_dir_exists(dir_path):
    try:
        dbutils.fs.ls(dir_path)
    except:
        dbutils.fs.mkdirs(dir_path)
        logger.info(f"Created directory: {dir_path}")

# Convert dbfs path to file system path
def dbfs_to_local(dbfs_path):
    if dbfs_path.startswith("dbfs:/"):
        return "/dbfs" + dbfs_path[5:]
    return dbfs_path

def _read_excel_file_spark(abfss_path: str, sheet_name=0):
    """
    Read an .xlsx from ABFSS into a pandas DataFrame using dbutils approach.
    This approach works better in Databricks environments.
    """
    try:
        logger.debug(f"Starting to read Excel file from: {abfss_path}")
        
        # Use dbutils to check if file exists and get file info
        try:
            file_info = dbutils.fs.ls(abfss_path)
            if not file_info:
                logger.error(f"No files found at {abfss_path}")
                return None
        except Exception as e:
            logger.error(f"Error accessing file {abfss_path}: {e}")
            return None

        # For abfss files, use the Azure Blob Storage approach
        return _read_excel_file(abfss_path, os.path.basename(abfss_path))

    except Exception as e:
        logger.error(f"An error occurred while processing {abfss_path}: {e}")
        return None

def _validate_excel_file(stream, file_name):
    """Validate if the downloaded file is a valid Excel file"""
    try:
        # Check file size
        stream.seek(0, 2)  # Seek to end
        file_size = stream.tell()
        stream.seek(0)  # Reset to beginning
        
        if file_size == 0:
            logger.error("❌ File is empty")
            return False
        
        if file_size < 100:  # Excel files should be at least 100 bytes
            logger.warning("⚠️ File is very small, may be corrupted")
        
        # Check file signature (first few bytes)
        signature = stream.read(8)
        stream.seek(0)  # Reset to beginning
        
        # Check for Excel file signatures
        excel_signatures = [
            b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1',  # OLE2 (older Excel)
            b'PK\x03\x04',  # ZIP-based (newer Excel)
        ]
        
        is_valid_excel = any(signature.startswith(sig) for sig in excel_signatures)
        
        if not is_valid_excel:
            logger.warning("⚠️ File doesn't appear to be a valid Excel file based on signature")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error validating file: {e}")
        return False

def _read_excel_file(file_path: str, file_name: str):
    """Read Excel file using Azure Blob Storage client and pandas (fallback method)"""
    try:
        # Extract blob info from abfss path
        # Format: abfss://container@account.dfs.core.windows.net/path
        match = re.match(r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)', file_path)
        if not match:
            logger.error(f"❌ Invalid abfss path format: {file_path}")
            return None
        
        container_name, account_name, blob_path = match.groups()
        
        # Get connection string from config
        connection_string = CONNECTION_STRING
        if not connection_string:
            logger.error("❌ Azure connection string not found in config")
            return None
        
        # Connect to blob storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # Download file to memory
        stream = BytesIO()
        blob_data = blob_client.download_blob().readall()
        stream.write(blob_data)
        stream.seek(0)
        
        # Validate the file before attempting to read
        if not _validate_excel_file(stream, file_name):
            logger.error("❌ File validation failed")
            return None
        
        # Read with pandas
        # Determine the appropriate engine based on file extension
        file_extension = os.path.splitext(file_name)[1].lower()
        if file_extension == '.xls':
            engine = 'xlrd'
        elif file_extension == '.xlsx':
            engine = 'openpyxl'
        else:
            # Default to xlrd for unknown extensions
            engine = 'xlrd'
        
        try:
            pandas_df = pd.read_excel(stream, sheet_name=0, engine=engine)  # Read first sheet
            logger.info(f"✅ Successfully read Excel file: {file_name}, shape: {pandas_df.shape}")
            return pandas_df
        except Exception as pandas_error:
            logger.error(f"❌ Pandas read_excel failed with engine {engine}: {pandas_error}")
            logger.error(f"🔍 Full pandas error: {traceback.format_exc()}")
            
            # Check if it's the specific "array index out of range" error
            if "array index out of range" in str(pandas_error).lower():
                logger.warning("⚠️ Detected 'array index out of range' error - file may be corrupted or have unusual format")
                logger.error(f"❌ Skipping corrupted file: {file_name}")
                return None
            
            # Try alternative engines for .xls files
            if file_extension == '.xls':
                stream.seek(0)  # Reset stream position
                logger.error(f"❌ Cannot read .xls file {file_name} - file may be corrupted")
                return None
            
            # Re-raise the original error if all attempts failed
            raise pandas_error
                
    except Exception as e:
        logger.error(f"❌ Error reading Excel file {file_path}: {e}")
        logger.error(f"🔍 Full error details: {traceback.format_exc()}")
        return None

# Extract company name from filename
def extract_company_name(filename):
    # Remove file extension
    filename = os.path.splitext(filename)[0]
    
    # Split by underscore
    parts = filename.split('_')
    
    # Find the part that comes before _Customers_ or _Suppliers_
    company_name = ""
    for i, part in enumerate(parts):
        if part in ["Customers", "Suppliers"] and i > 0:
            company_name = parts[i-1]
            break
    
    # If not found, use a different approach
    if not company_name:
        # Try to extract from SPGlobal_CompanyName_...
        if filename.startswith("SPGlobal_"):
            # Remove SPGlobal_
            rest = filename[9:]
            # Find the part before the next underscore
            if "_" in rest:
                company_name = rest.split("_")[0]
            else:
                company_name = rest
    
    return company_name

# Extract data with pandas as a fallback method
def extract_data_with_pandas(file_path, file_type, company_name, debug=False):
    data_records = []
    
    try:
        # Try Spark-based reading first, fallback to Azure Blob Storage
        df = _read_excel_file_spark(file_path)
        if df is None:
            df = _read_excel_file(file_path, os.path.basename(file_path))
        
        if df is None:
            logger.error(f"❌ Failed to read Excel file: {file_path}")
            return data_records
        
        # Convert DataFrame to rows for processing
        rows = [df.columns.tolist()] + df.values.tolist()
        
        # For multi-sheet processing, we need to read the file again using Azure Blob Storage
        # Use Azure Blob Storage method for multi-sheet processing
        try:
            # Extract blob info from abfss path
            match = re.match(r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)', file_path)
            if not match:
                if debug:
                    logger.debug(f"Invalid abfss path format: {file_path}")
                return data_records
            
            container_name, account_name, blob_path = match.groups()
            
            # Get connection string from config
            connection_string = CONNECTION_STRING
            if not connection_string:
                if debug:
                    logger.debug("Azure connection string not found in config")
                return data_records
            
            # Connect to blob storage
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            
            # Download file to memory for ExcelFile processing
            stream = BytesIO()
            stream.write(blob_client.download_blob().readall())
            stream.seek(0)
            
            # Try to read the Excel file with pandas for sheet processing
            excel_data = pd.ExcelFile(stream)
        except Exception as e:
            if debug:
                logger.debug(f"Azure Blob Storage reading failed: {e}")
            return data_records
        
        # First, read the entire sheet to identify section markers
        for sheet_name in excel_data.sheet_names:
            
            # Read the sheet without headers using the stream
            df_raw = pd.read_excel(stream, sheet_name=sheet_name, header=None)
            
            # Find rows that contain section markers
            section_markers = {}
            for i, row in df_raw.iterrows():
                for j, cell in enumerate(row):
                    if isinstance(cell, str) and "disclosed" in cell.lower():
                        section_markers[i] = cell.strip()
                        break
            
            # Process each section
            for section_row, section_name in section_markers.items():
                # Check if this section has "No Data Available"
                has_no_data = False
                for i in range(section_row + 1, min(section_row + 5, len(df_raw))):
                    for j, cell in enumerate(df_raw.iloc[i]):
                        if isinstance(cell, str) and "no data available" in cell.lower():
                            has_no_data = True
                            break
                    if has_no_data:
                        break
                
                # Skip this section if it has no data
                if has_no_data:
                    continue
                
                # Look for header row after the section marker
                header_row = None
                for i in range(section_row + 1, min(section_row + 10, len(df_raw))):
                    for j, cell in enumerate(df_raw.iloc[i]):
                        key_term = "customer name" if file_type == "customer" else "supplier name"
                        if isinstance(cell, str) and key_term in cell.lower():
                            header_row = i
                            break
                    if header_row is not None:
                        break
                
                if header_row is not None:
                    # Find the next section marker (if any)
                    next_section_row = float('inf')
                    for row in section_markers.keys():
                        if row > header_row and row < next_section_row:
                            next_section_row = row
                    
                    # Read the data with the header row
                    try:
                        # Calculate number of rows to read
                        nrows = next_section_row - header_row - 1 if next_section_row < float('inf') else None
                        
                        df_section = pd.read_excel(stream, sheet_name=sheet_name, header=header_row, nrows=nrows)
                        
                        # Clean the dataframe
                        df_section = df_section.dropna(how='all')
                        
                        # Add company name column
                        df_section["Company Name"] = company_name
                        
                        # Add disclosed information column from the section name
                        df_section["Disclosed Information"] = section_name
                        
                        # Try to extract description
                        description = ""
                        for i in range(section_row):
                            for j, cell in enumerate(df_raw.iloc[i]):
                                if isinstance(cell, str) and "KEY:" in cell:
                                    description = cell.strip()
                                    break
                            if description:
                                break
                        
                        # Add description column
                        df_section["Description"] = description
                        
                        # Convert to records
                        records = df_section.to_dict('records')
                        
                        # Filter out non-data rows
                        for record in records:
                            # Skip rows without key column data
                            key_field = "Customer Name" if file_type == "customer" else "Supplier Name"
                            if key_field not in record or record[key_field] == "":
                                continue
                            
                            # Skip rows that look like headers or section titles
                            key_value = str(record[key_field]).lower()
                            if "disclosed" in key_value or "name" in key_value or "no data" in key_value:
                                continue
                            
                            # Add the record
                            data_records.append(record)
                        
                    except Exception as e:
                        if debug:
                            logger.debug(f"Error processing section {section_name}: {e}")
            
            # If no data found using section approach, try a more direct approach
            if not data_records:
                
                # Try different header rows
                for header_row in range(0, 30):
                    try:
                        df = pd.read_excel(stream, sheet_name=sheet_name, header=header_row)
                        
                        # Check if this looks like a data table
                        key_column = "Customer Name" if file_type == "customer" else "Supplier Name"
                        
                        if key_column in df.columns:
                            # Check if there's a "No Data Available" right after the header
                            no_data_after_header = False
                            if header_row + 1 < len(df_raw):
                                for j, cell in enumerate(df_raw.iloc[header_row + 1]):
                                    if isinstance(cell, str) and "no data" in cell.lower():
                                        no_data_after_header = True
                                        break
                            
                            if no_data_after_header:
                                # Skip this header as it has no data
                                continue
                            
                            # Clean the dataframe
                            df = df.dropna(how='all')
                            
                            # Add company name column
                            df["Company Name"] = company_name
                            
                            # Try to find a section name
                            section_name = ""
                            for i in range(max(0, header_row - 5), header_row):
                                if i < len(df_raw):
                                    for j, cell in enumerate(df_raw.iloc[i]):
                                        if isinstance(cell, str) and "disclosed" in cell.lower():
                                            section_name = cell.strip()
                                            break
                                if section_name:
                                    break
                            
                            # Add disclosed information column
                            df["Disclosed Information"] = section_name
                            
                            # Try to extract description
                            description = ""
                            for i in range(header_row):
                                if i < len(df_raw):
                                    for j, cell in enumerate(df_raw.iloc[i]):
                                        if isinstance(cell, str) and "KEY:" in cell:
                                            description = cell.strip()
                                            break
                                if description:
                                    break
                            
                            # Add description column
                            df["Description"] = description
                            
                            # Convert to records
                            records = df.to_dict('records')
                            
                            # Filter out non-data rows
                            for record in records:
                                # First replace NaN with empty strings
                                for key, value in record.items():
                                    if pd.isna(value):
                                        record[key] = ""
                                
                                # Skip rows that look like headers or section titles
                                key_value = str(record[key_column]).lower()
                                if "disclosed" in key_value or "name" in key_value or "no data" in key_value:
                                    continue
                                
                                # Round specific numeric columns to 2 decimal places
                                numeric_columns = [
                                    "Supplier Expense ($M)", "Supplier Expense (%)", 
                                    "Min %", "Max %", "Min Value ($M)", "Max Value ($M)",
                                    "Customer Revenue ($M)", "Customer Revenue (%)"
                                ]
                                
                                for col in numeric_columns:
                                    if col in record and not pd.isna(record[col]) and isinstance(record[col], (int, float)):
                                        record[col] = round(record[col], 2)
                                
                                # Add the record
                                data_records.append(record)
                            
                            if data_records:
                                return data_records
                    
                    except Exception as e:
                        if debug:
                            logger.debug(f"Error with pandas at header row {header_row}: {e}")
                        continue
    
    except Exception as e:
        if debug:
            logger.debug(f"Error in extract_data_with_pandas: {e}")
        raise  # Re-raise the exception to be caught by the caller
    
    return data_records

def extract_data_from_excel(file_path, file_type, company_name, debug=False):
    data_records = []
    
    try:
        # Try Spark-based reading first, fallback to Azure Blob Storage
        df = _read_excel_file_spark(file_path)
        if df is None:
            df = _read_excel_file(file_path, os.path.basename(file_path))
        
        if df is None:
            logger.error(f"❌ Failed to read Excel file: {file_path}")
            return data_records
        
        # Convert DataFrame to rows for processing
        rows = [df.columns.tolist()] + df.values.tolist()
        
        # For openpyxl processing, we need to read the file again using Azure Blob Storage
        # Use Azure Blob Storage method for openpyxl processing
        try:
            # Extract blob info from abfss path
            match = re.match(r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)', file_path)
            if not match:
                if debug:
                    logger.debug(f"Invalid abfss path format: {file_path}")
                return data_records
            
            container_name, account_name, blob_path = match.groups()
            
            # Get connection string from config
            connection_string = CONNECTION_STRING
            if not connection_string:
                if debug:
                    logger.debug("Azure connection string not found in config")
                return data_records
            
            # Connect to blob storage
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            
            # Download file to memory for openpyxl processing
            stream = BytesIO()
            stream.write(blob_client.download_blob().readall())
            stream.seek(0)
        except Exception as e:
            if debug:
                logger.debug(f"Azure Blob Storage reading failed: {e}")
            return data_records
        
        # Check if file is .xls format - if so, skip openpyxl and use pandas directly
        file_extension = os.path.splitext(os.path.basename(file_path))[1].lower()
        
        if file_extension == '.xls':
            # For .xls files, use pandas processing directly
            try:
                data_records = extract_data_with_pandas(file_path, file_type, company_name, debug)
            except Exception as pandas_error:
                logger.error(f"❌ Pandas processing failed: {pandas_error}")
                data_records = []
        else:
            # For .xlsx files, use pandas processing directly (simpler and more reliable)
            try:
                data_records = extract_data_with_pandas(file_path, file_type, company_name, debug)
            except Exception as pandas_error:
                logger.error(f"❌ Pandas processing failed: {pandas_error}")
                data_records = []
                
    except Exception as e:
        if debug:
            logger.debug(f"Error in extract_data_from_excel: {e}")
        raise  # Re-raise the exception to be caught by the caller
    
    return data_records

# Process a single Excel file
def process_single_file(file_path, output_directory, debug=False):
    filename = os.path.basename(file_path)
    
    try:
        # Extract company name from filename
        company_name = extract_company_name(filename)
        
        # Determine if file contains customer or supplier data based on filename
        is_customer_file = "_Customers_" in filename
        is_supplier_file = "_Suppliers_" in filename
        
        if not (is_customer_file or is_supplier_file):
            if debug:
                logger.debug(f"Cannot determine file type for {filename}, skipping...")
            
            return ("skip", filename, "", {
                "reason": "Cannot determine file type (missing '_Customers_' or '_Suppliers_' in filename)"
            })
        
        # Extract data from the file
        file_data = {"customer_data": [], "supplier_data": []}
        extraction_status = {"customer": False, "supplier": False}
        extraction_reason = {"customer": "", "supplier": ""}
        
        if is_customer_file:
            try:
                customer_data = extract_data_from_excel(file_path, "customer", company_name, debug)
                
                if customer_data:
                    file_data["customer_data"] = customer_data
                    extraction_status["customer"] = True
                else:
                    extraction_reason["customer"] = "No customer data found"
            except Exception as e:
                extraction_reason["customer"] = f"Error: {str(e)}"
        
        if is_supplier_file:
            try:
                supplier_data = extract_data_from_excel(file_path, "supplier", company_name, debug)
                
                if supplier_data:
                    file_data["supplier_data"] = supplier_data
                    extraction_status["supplier"] = True
                else:
                    extraction_reason["supplier"] = "No supplier data found"
            except Exception as e:
                extraction_reason["supplier"] = f"Error: {str(e)}"
        
        # Determine success or failure
        if (is_customer_file and extraction_status["customer"]) or (is_supplier_file and extraction_status["supplier"]):
            return ("success", filename, company_name, file_data)
        else:
            reasons = []
            if is_customer_file and not extraction_status["customer"]:
                reasons.append(f"Customer data: {extraction_reason['customer']}")
            if is_supplier_file and not extraction_status["supplier"]:
                reasons.append(f"Supplier data: {extraction_reason['supplier']}")
            
            return ("fail", filename, company_name, {
                "reasons": reasons
            })
            
    except Exception as e:
        error_traceback = traceback.format_exc()
        return ("fail", filename, extract_company_name(filename), {
            "reasons": [f"Unexpected error: {str(e)}"],
            "traceback": error_traceback
        })

# Main processing function
def process_excel_files():
    # Create timestamp for this processing run
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Initialize tracking lists
    successful_files = []
    failed_files = []
    skipped_files = []
    processing_summary = {
        "total_files": 0,
        "successful_files": 0,
        "failed_files": 0,
        "skipped_files": 0,
        "customer_records": 0,
        "supplier_records": 0
    }
    
    # Ensure output and log directories exist
    ensure_dir_exists(OUTPUT_DIR)
    ensure_dir_exists(LOG_DIR)
    
    # Initialize lists to store combined data
    all_customer_data = []
    all_supplier_data = []
    
    # Define columns for the output CSVs
    customer_columns = [
        "Company Name", "Description", "Disclosed Information", 
        "Customer Name", "Supplier Name", "Relationship Type", 
        "Primary Industry", "Geography", "Start Date", "End Date", 
        "Customer Revenue ($M)", "Customer Revenue (%)", "Min %", "Max %",
        "Min Value ($M)", "Max Value ($M)", "Source"
    ]
    
    supplier_columns = [
        "Company Name", "Description", "Disclosed Information", 
        "Supplier Name", "Customer Name", "Relationship Type", 
        "Primary Industry", "Geography", "Start Date", "End Date", 
        "Supplier Expense ($M)", "Supplier Expense (%)", "Min %", "Max %",
        "Min Value ($M)", "Max Value ($M)", "Source"
    ]
    
    # Get list of Excel files from Azure Blob Storage
    excel_files = []
    try:
        file_list = dbutils.fs.ls(INPUT_DIR)
        for file_info in file_list:
            if not file_info.isDir() and (file_info.path.endswith('.xls') or file_info.path.endswith('.xlsx')):
                excel_files.append(file_info.path)
    except Exception as e:
        logger.error(f"Error listing files in {INPUT_DIR}: {e}")
        return
    
    # Check for previously processed files to avoid reprocessing
    processed_files_path = f"{LOG_DIR}/processed_files.txt"
    previously_processed = set()
    
    try:
        # Read the processed_files.txt if it exists
        processed_content = dbutils.fs.head(processed_files_path, 1000000)  # Read up to 1MB
        if processed_content:
            previously_processed = set(processed_content.decode('utf-8').strip().split('\n'))
            previously_processed.discard('')  # Remove empty lines
            logger.info(f"Found {len(previously_processed)} previously processed files")
    except Exception as e:
        logger.info("No processed_files.txt found, will process all files")
    
    # Filter out previously processed files
    files_to_process = [f for f in excel_files if os.path.basename(f) not in previously_processed]
    skipped_count = len(excel_files) - len(files_to_process)
    
    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} previously processed files")
    
    excel_files = files_to_process
    
    if not excel_files:
        logger.warning(f"No Excel files found in {INPUT_DIR}")
        return
    
    processing_summary["total_files"] = len(excel_files) + skipped_count
    logger.info(f"Found {len(excel_files) + skipped_count} Excel file(s) total")
    if skipped_count > 0:
        logger.info(f"Processing {len(excel_files)} new files, skipping {skipped_count} previously processed files")
    
    # Process files one by one
    logger.info("Processing files...")
    for file_path in excel_files:
        filename = os.path.basename(file_path)
        logger.info(f"Processing file: {filename}")
        
        result = process_single_file(file_path, OUTPUT_DIR, debug_mode)
        
        if result is None:
            continue
            
        status, filename, company_name, file_data = result
        
        if status == "success":
            customer_data = file_data.get("customer_data", [])
            supplier_data = file_data.get("supplier_data", [])
            
            if customer_data:
                all_customer_data.extend(customer_data)
                successful_files.append({
                    "filename": filename,
                    "company": company_name,
                    "customer_records": len(customer_data),
                    "supplier_records": 0
                })
                processing_summary["customer_records"] += len(customer_data)
            
            if supplier_data:
                all_supplier_data.extend(supplier_data)
                # Check if we already added this file to successful_files
                if customer_data:
                    # Update the existing entry
                    for entry in successful_files:
                        if entry["filename"] == filename:
                            entry["supplier_records"] = len(supplier_data)
                            break
                else:
                    successful_files.append({
                        "filename": filename,
                        "company": company_name,
                        "customer_records": 0,
                        "supplier_records": len(supplier_data)
                    })
                processing_summary["supplier_records"] += len(supplier_data)
            
            processing_summary["successful_files"] += 1
            
            # Add to processed files list
            previously_processed.add(filename)
            
        elif status == "skip":
            skipped_files.append({
                "filename": filename,
                "reason": file_data.get("reason", "Unknown reason")
            })
            processing_summary["skipped_files"] += 1
            
        elif status == "fail":
            failed_files.append({
                "filename": filename,
                "company": company_name,
                "reasons": file_data.get("reasons", ["Unknown error"]),
                "traceback": file_data.get("traceback", "")
            })
            processing_summary["failed_files"] += 1
    
    if all_customer_data:
        # Convert records to DataFrame
        customer_df = pd.DataFrame(all_customer_data)
        
        # Ensure all required columns exist
        for col in customer_columns:
            if col not in customer_df.columns:
                customer_df[col] = ""
        
        # Reorder columns
        customer_df = customer_df[customer_columns]
        
        # Drop duplicates
        customer_df.drop_duplicates(inplace=True)
        
        # Fill NaN values with empty strings
        customer_df.fillna("", inplace=True)
        
        # Write CSV directly using pandas to ensure proper CSV format
        # Convert to CSV string
        csv_content = customer_df.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='"')
        
        # Verify CSV format (first few lines should be readable)
        lines = csv_content.split('\n')[:3]
        logger.info(f"CSV format verification - First 3 lines: {lines}")
        
        # Write directly to DBFS
        output_path = f"{OUTPUT_DIR}/customers.csv"
        dbutils.fs.put(output_path, csv_content, overwrite=True)
        
        logger.info(f"Saved {len(all_customer_data)} customer records to {OUTPUT_DIR}/customers.csv")

    # Write supplier data to CSV
    if all_supplier_data:
        # Convert records to DataFrame
        supplier_df = pd.DataFrame(all_supplier_data)
        
        # Ensure all required columns exist
        for col in supplier_columns:
            if col not in supplier_df.columns:
                supplier_df[col] = ""
        
        # Reorder columns
        supplier_df = supplier_df[supplier_columns]
        
        # Drop duplicates
        supplier_df.drop_duplicates(inplace=True)
        
        # Fill NaN values with empty strings
        supplier_df.fillna("", inplace=True)
        
        # Write CSV directly using pandas to ensure proper CSV format
        # Convert to CSV string
        csv_content = supplier_df.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='"')
        
        # Verify CSV format (first few lines should be readable)
        lines = csv_content.split('\n')[:3]
        logger.info(f"CSV format verification - First 3 lines: {lines}")
        
        # Write directly to DBFS
        output_path = f"{OUTPUT_DIR}/suppliers.csv"
        dbutils.fs.put(output_path, csv_content, overwrite=True)
        
        logger.info(f"Saved {len(all_supplier_data)} supplier records to {OUTPUT_DIR}/suppliers.csv")    
    
    # Generate processing summary and logs
    summary_log = f"{LOG_DIR}/processing_summary_{timestamp}.txt"
    
    # Create a summary log as string
    summary_content = f"""===== Excel Data Extraction Summary =====
Run Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Input Directory: {INPUT_DIR}
Output Directory: {OUTPUT_DIR}

=== Processing Statistics ===
Total files found: {processing_summary['total_files']}
Previously processed (skipped): {skipped_count}
New files processed: {len(excel_files)}
Successfully processed: {processing_summary['successful_files']}
Failed to process: {processing_summary['failed_files']}
Skipped (indeterminate type): {processing_summary['skipped_files']}
Total customer records extracted: {processing_summary['customer_records']}
Total supplier records extracted: {processing_summary['supplier_records']}

=== Successfully Processed Files ===
"""
    
    for file in successful_files:
        summary_content += f"- {file['filename']} (Company: {file['company']})\n"
        if file.get('customer_records', 0) > 0:
            summary_content += f"  Customer records: {file['customer_records']}\n"
        if file.get('supplier_records', 0) > 0:
            summary_content += f"  Supplier records: {file['supplier_records']}\n"
    
    summary_content += "\n=== Failed Files ===\n"
    for file in failed_files:
        summary_content += f"- {file['filename']} (Company: {file['company']})\n"
        summary_content += "  Reasons:\n"
        for reason in file['reasons']:
            summary_content += f"  - {reason}\n"
    
    summary_content += "\n=== Skipped Files ===\n"
    for file in skipped_files:
        summary_content += f"- {file['filename']}\n"
        summary_content += f"  Reason: {file.get('reason', 'Unknown')}\n"
    
    # Write summary log to DBFS
    dbutils.fs.put(summary_log, summary_content, overwrite=True)
    
    # Update processed_files.txt with newly processed files
    if previously_processed:
        processed_content = '\n'.join(sorted(previously_processed))
        dbutils.fs.put(processed_files_path, processed_content, overwrite=True)
        logger.info(f"Updated processed_files.txt with {len(previously_processed)} files")
    
    logger.info(f"\nSummary report written to: {summary_log}")
    
    # Also save detailed logs as CSV files
    # Convert tracking lists to DataFrames
    successful_df = pd.DataFrame([{
        'filename': f['filename'],
        'company': f['company'],
        'customer_records': f.get('customer_records', 0),
        'supplier_records': f.get('supplier_records', 0)
    } for f in successful_files])

    failed_df = pd.DataFrame([{
        'filename': f['filename'],
        'company': f['company'],
        'reasons': '; '.join(f['reasons'])
    } for f in failed_files])

    skipped_df = pd.DataFrame([{
        'filename': f['filename'],
        'reason': f.get('reason', 'Unknown')
    } for f in skipped_files])
    
    # Save to CSV using direct pandas method
    if not successful_df.empty:
        csv_content = successful_df.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='"')
        dbutils.fs.put(f"{LOG_DIR}/successful_files_{timestamp}.csv", csv_content, overwrite=True)
    
    if not failed_df.empty:
        csv_content = failed_df.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='"')
        dbutils.fs.put(f"{LOG_DIR}/failed_files_{timestamp}.csv", csv_content, overwrite=True)
    
    if not skipped_df.empty:
        csv_content = skipped_df.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='"')
        dbutils.fs.put(f"{LOG_DIR}/skipped_files_{timestamp}.csv", csv_content, overwrite=True)
    
    # Print summary to console
    logger.info("\n===== Processing Summary =====")
    logger.info(f"Total files: {processing_summary['total_files']}")
    logger.info(f"Successfully processed: {processing_summary['successful_files']}")
    logger.info(f"Failed to process: {processing_summary['failed_files']}")
    logger.info(f"Skipped (indeterminate type): {processing_summary['skipped_files']}")
    logger.info(f"Total customer records extracted: {processing_summary['customer_records']}")
    logger.info(f"Total supplier records extracted: {processing_summary['supplier_records']}")
    
    return {
        "successful_files": successful_files,
        "failed_files": failed_files,
        "skipped_files": skipped_files,
        "summary": processing_summary
    }

# Execute the main function
try:
    start_time = datetime.datetime.now()
    logger.info(f"Processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    result = process_excel_files()
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    logger.info(f"Processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total duration: {duration}")
    
    if result:
        logger.info(f"Successfully processed {result['summary']['successful_files']} of {result['summary']['total_files']} files")
        logger.info(f"Extracted {result['summary']['customer_records']} customer records and {result['summary']['supplier_records']} supplier records")
    
    print("\nProcessing complete!")
    print(f"Duration: {duration}")
    
except Exception as e:
    logger.error(f"Fatal error during processing: {e}")
    logger.error(traceback.format_exc())
    print(f"\nError occurred: {e}")