from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, pandas_udf, lit
from pyspark.sql.types import StringType, StructType, StructField
import sys
import os
import pandas as pd
import tempfile

def is_databricks_environment():
    """Check if running in Databricks environment"""
    try:
        # Check if dbutils is available (Databricks specific)
        import dbutils
        return True
    except:
        return False

def get_or_create_spark_session(app_name="DataDeduplication"):
    """Get existing Spark session or create new one for Azure Databricks"""
    try:
        # Check if we're in Databricks environment
        if is_databricks_environment():
            # In Databricks, use the existing session
            existing_spark = SparkSession.getActiveSession()
            if existing_spark:
                print(f"✓ Using existing Databricks Spark session")
                
                # Configure the existing session for better performance
                existing_spark.conf.set("spark.sql.adaptive.enabled", "true")
                existing_spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                
                return existing_spark
        
        # Fallback: create new session (for non-Databricks environments)
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
            
        print(f"✓ Created new Spark session: {app_name}")
        return spark
        
    except Exception as e:
        print(f"Error with Spark session: {e}")
        raise

def cleanup_spark_session(spark, is_databricks_session=True):
    """Properly cleanup Spark session and resources"""
    try:
        if spark:
            print("Cleaning up Spark session resources...")
            
            # Clear cache (safe for both Databricks and standalone)
            spark.catalog.clearCache()
            
            # In Databricks, DON'T stop the session as it's shared
            if not is_databricks_session:
                print("Stopping Spark session...")
                spark.stop()
                SparkSession._instantiatedSession = None
            else:
                print("✓ Cleared Spark cache (keeping session active for Databricks)")
            
            print("✓ Spark session cleanup completed")
            
    except Exception as e:
        print(f"Warning: Error during Spark cleanup: {e}")

def cleanup_temp_directories(storage_account, container, temp_dirs):
    """Clean up temporary directories in blob storage"""
    try:
        for temp_dir in temp_dirs:
            temp_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{temp_dir}"
            try:
                dbutils.fs.rm(temp_path, True)
                print(f"✓ Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                print(f"Warning: Could not clean up {temp_dir}: {e}")
    except Exception as e:
        print(f"Warning: Error during temp directory cleanup: {e}")

def analyze_file_formats(blob_path):
    """
    Analyze all file formats in the folder and return counts
    """
    try:
        # List files in the directory to detect format
        file_list = dbutils.fs.ls(blob_path)
        
        format_counts = {
            'excel': [],
            'csv': [],
            'parquet': [],
            'json': []
        }
        
        for file_info in file_list:
            if not file_info.isDir():
                file_name = file_info.name.lower()
                if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                    format_counts['excel'].append(file_info.path)
                elif file_name.endswith('.csv'):
                    format_counts['csv'].append(file_info.path)
                elif file_name.endswith('.parquet'):
                    format_counts['parquet'].append(file_info.path)
                elif file_name.endswith('.json'):
                    format_counts['json'].append(file_info.path)
        
        # Print summary
        total_files = sum(len(files) for files in format_counts.values())
        print(f"File analysis - Total files: {total_files}")
        for fmt, files in format_counts.items():
            if files:
                print(f"  {fmt.upper()}: {len(files)} files")
        
        return format_counts
    except Exception as e:
        print(f"Could not analyze file formats: {e}")
        return {'excel': [], 'csv': [], 'parquet': [], 'json': []}

# Excel processing functions removed - using CSV-only approach for reliability

def read_blob_folder(spark, storage_account, container, input_folder):
    """
    Read all files from Azure blob folder into DataFrame - handles mixed formats
    """
    # Build path exactly like sp_business.py
    blob_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{input_folder}"
    
    print(f"Reading files from: {blob_path}")
    
    # Analyze all file formats in the folder
    format_analysis = analyze_file_formats(blob_path)
    
    # Collect all DataFrames from different formats
    dataframes = []
    
    # Process Excel files if any exist
    if format_analysis['excel']:
        print(f"📊 Found {len(format_analysis['excel'])} Excel files")
        print("🚀 DIRECT SPARK EXCEL PROCESSING: Using com.crealytics (compatible version installed)")
        
        excel_dataframes = []
        failed_excel_files = []
        
        for excel_file in format_analysis['excel']:
            file_name = excel_file.split('/')[-1]
            print(f"   📄 Processing: {file_name}")
            
            try:
                # Try direct Spark Excel reading first (fastest and most efficient)
                print(f"      Attempting direct Spark Excel reading...")
                excel_df = spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("dataAddress", "'Sheet1'!A1") \
                    .option("treatEmptyValuesAsNulls", "true") \
                    .option("usePlainNumberFormat", "true") \
                    .load(excel_file)
                
                # Get stats
                row_count = excel_df.count()
                col_count = len(excel_df.columns)
                
                excel_dataframes.append(excel_df)
                print(f"      ✅ SUCCESS with Spark Excel: {file_name} ({row_count:,} rows, {col_count} cols)")
                
            except Exception as spark_error:
                print(f"      ❌ Spark Excel failed: {str(spark_error)}")
                print(f"      🔄 Falling back to pandas conversion...")
                
                # Fallback: pandas approach
                try:
                    # Copy Excel file to local temp
                    import uuid
                    unique_id = str(uuid.uuid4())[:8]
                    local_excel = f"/tmp/{unique_id}_{file_name}"
                    local_csv = f"/tmp/{unique_id}_{file_name}.csv"
                    
                    # Download Excel file
                    dbutils.fs.cp(excel_file, f"file:{local_excel}")
                    
                    # Convert with pandas using multiple engine attempts
                    import pandas as pd
                    pdf = None
                    
                    if file_name.lower().endswith('.xlsx'):
                        engines_to_try = ['openpyxl', 'xlrd']
                    else:
                        engines_to_try = ['xlrd', 'openpyxl']
                    
                    for engine in engines_to_try:
                        try:
                            print(f"         Trying pandas engine: {engine}")
                            pdf = pd.read_excel(local_excel, engine=engine)
                            print(f"         ✅ Success with {engine}")
                            break
                        except Exception as e:
                            print(f"         ❌ {engine} failed: {str(e)}")
                            continue
                    
                    if pdf is None:
                        raise Exception("All pandas engines failed")
                    
                    # Save as CSV
                    pdf.to_csv(local_csv, index=False, encoding='utf-8', quoting=1, escapechar='\\')
                    
                    # Read CSV back with Spark
                    excel_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{local_csv}")
                    excel_dataframes.append(excel_df)
                    
                    print(f"      ✅ SUCCESS with pandas fallback: {file_name} ({len(pdf):,} rows, {len(pdf.columns)} cols)")
                    
                    # Cleanup temp files
                    import os
                    for temp_file in [local_excel, local_csv]:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                            
                except Exception as pandas_error:
                    failed_excel_files.append(file_name)
                    print(f"      ❌ Both Spark and pandas failed: {str(pandas_error)}")
                    
                    # Cleanup temp files on failure
                    try:
                        import os
                        for temp_file in [local_excel, local_csv]:
                            if os.path.exists(temp_file):
                                os.remove(temp_file)
                    except:
                        pass
        
        # Add successful Excel dataframes
        if excel_dataframes:
            dataframes.extend(excel_dataframes)
            print(f"🎉 Successfully processed {len(excel_dataframes)} Excel files!")
        
        # Report failed files
        if failed_excel_files:
            print(f"⚠️ Failed to process {len(failed_excel_files)} Excel files:")
            for failed_file in failed_excel_files:
                print(f"   📄 {failed_file}")
            print("💡 RECOMMENDATION: Check file format or convert to CSV manually")
    
    # Process CSV files if any exist
    if format_analysis['csv']:
        print(f"Processing {len(format_analysis['csv'])} CSV files...")
        csv_paths = format_analysis['csv']
        
        # Read all CSV files at once if they're in the same directory
        # Or read them individually and union them
        csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_paths)
        dataframes.append(csv_df)
    
    # Process Parquet files if any exist
    if format_analysis['parquet']:
        print(f"Processing {len(format_analysis['parquet'])} Parquet files...")
        parquet_paths = format_analysis['parquet']
        parquet_df = spark.read.parquet(parquet_paths)
        dataframes.append(parquet_df)
    
    # Process JSON files if any exist
    if format_analysis['json']:
        print(f"Processing {len(format_analysis['json'])} JSON files...")
        json_paths = format_analysis['json']
        json_df = spark.read.json(json_paths)
        dataframes.append(json_df)
    
    # Combine all DataFrames
    if not dataframes:
        raise ValueError("No supported files found in the directory")
    elif len(dataframes) == 1:
        combined_df = dataframes[0]
    else:
        print(f"Combining {len(dataframes)} DataFrames from different formats...")
        # Union all DataFrames (they should have the same schema)
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
    
    print(f"✓ Successfully loaded and combined all files")
    return combined_df

def print_dedup_stats(original_df, dedup_df):
    """Print deduplication statistics"""
    original_count = original_df.count()
    dedup_count = dedup_df.count()
    duplicates_removed = original_count - dedup_count
    
    print("=" * 50)
    print("DEDUPLICATION STATISTICS")
    print("=" * 50)
    print(f"Original records: {original_count:,}")
    print(f"Deduplicated records: {dedup_count:,}")
    print(f"Duplicates removed: {duplicates_removed:,}")
    print(f"Duplicate percentage: {(duplicates_removed/original_count)*100:.2f}%")
    print("=" * 50)

def deduplicate_data(df, subset_columns=None):
    """
    Deduplicate DataFrame
    
    Args:
        df: Input DataFrame
        subset_columns: List of columns to consider for deduplication. If None, uses all columns
    """
    print("Starting deduplication process...")
    
    if subset_columns:
        print(f"Deduplicating based on columns: {subset_columns}")
        dedup_df = df.dropDuplicates(subset_columns)
    else:
        print("Deduplicating based on all columns")
        dedup_df = df.dropDuplicates()
    
    return dedup_df

def save_to_blob(df, storage_account, container, output_folder, file_format="xlsx"):
    """
    Save DataFrame to Azure blob storage in specified format using distributed processing
    """
    # Build path exactly like sp_business.py
    output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{output_folder}"
    
    print(f"Saving deduplicated data to: {output_path}")
    print(f"Output format: {file_format}")
    
    if file_format.lower() == "xlsx" or file_format.lower() == "excel":
        # First save as CSV using Spark (distributed), then convert to Excel
        temp_csv_path = f"{output_path}_temp_csv"
        
        print("Saving as CSV using Spark distributed processing...")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_csv_path)
        
        # Find the actual CSV file (Spark creates part files)
        csv_files = dbutils.fs.ls(temp_csv_path)
        csv_file = [f for f in csv_files if f.name.startswith('part-') and f.name.endswith('.csv')][0]
        
        print("Converting CSV to Excel...")
        # Copy CSV to workspace temp for pandas conversion
        workspace_csv_path = f"/Workspace/tmp/temp_data.csv"
        workspace_excel_path = f"/Workspace/tmp/deduplicated_data.xlsx"
        
        # Ensure workspace temp directory exists
        dbutils.fs.mkdirs("file:/Workspace/tmp/")
        
        # Copy CSV from blob to workspace
        dbutils.fs.cp(csv_file.path, f"file:{workspace_csv_path}")
        
        try:
            # Convert CSV to Excel using pandas with multiple engine support
            print("Reading CSV for Excel conversion...")
            pdf = pd.read_csv(workspace_csv_path, 
                            on_bad_lines='skip',  # Skip problematic lines
                            quoting=1,            # Quote all fields
                            escapechar='\\')      # Handle escape characters
            print(f"Successfully read {len(pdf):,} rows from CSV")
            
            # Try multiple engines for Excel output
            excel_engines = ['openpyxl', 'xlsxwriter']
            excel_saved = False
            
            for engine in excel_engines:
                try:
                    print(f"Trying Excel output engine: {engine}")
                    pdf.to_excel(workspace_excel_path, 
                               index=False, 
                               sheet_name="Deduplicated_Data",
                               engine=engine)
                    print(f"✅ Excel saved successfully with {engine}")
                    excel_saved = True
                    break
                except Exception as e:
                    print(f"❌ {engine} failed: {str(e)}")
                    continue
            
            if not excel_saved:
                # Fallback: save as CSV if Excel fails
                csv_fallback = workspace_excel_path.replace('.xlsx', '.csv')
                pdf.to_csv(csv_fallback, index=False)
                print(f"⚠️ Excel failed, saved as CSV: {csv_fallback}")
                workspace_excel_path = csv_fallback
            
            # Copy Excel file back to blob storage
            final_excel_path = f"{output_path}/deduplicated_data.xlsx"
            dbutils.fs.cp(f"file:{workspace_excel_path}", final_excel_path)
            
            print(f"Excel file saved to: {final_excel_path}")
            
        finally:
            # Clean up temp files
            for temp_file in [workspace_csv_path, workspace_excel_path]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            # Clean up temp CSV directory
            dbutils.fs.rm(temp_csv_path, True)
        
    elif file_format.lower() == "csv":
        # Use Spark distributed write for CSV
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    elif file_format.lower() == "parquet":
        # Use Spark distributed write for Parquet
        df.write.mode("overwrite").parquet(output_path)
    elif file_format.lower() == "json":
        # Use Spark distributed write for JSON
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError(f"Unsupported output format: {file_format}")
    
    print("Data saved successfully!")

def main():
    """Main function to run deduplication process with proper resource management"""
    
    # Configuration - modify these values as needed
    STORAGE_ACCOUNT = "dataingestiondl"       # Your Azure storage account
    CONTAINER = "raw-data"                    # Your container name  
    INPUT_FOLDER = "8.HNI/30-April-2025"     # Input folder path
    OUTPUT_FOLDER = "8.HNI/dedup_output"     # Output folder path
    FILE_FORMAT = "xlsx"                      # Output format: xlsx, csv, parquet, json
    
    # Optional: specify columns for deduplication (None = all columns)
    DEDUP_COLUMNS = None  # e.g., ["column1", "column2"] or None for all columns
    
    # Initialize variables for cleanup
    spark = None
    temp_directories = []
    
    try:
        print("=" * 60)
        print("AZURE BLOB DEDUPLICATION PROCESS")
        print("=" * 60)
        
        # Get or create Spark session (Databricks compatible)
        spark = get_or_create_spark_session("Azure_Blob_Deduplication")
        
        print("Starting data processing...")
        print("-" * 50)
        
        # Read data from blob folder (auto-detect input format)
        print("📂 Reading input data...")
        original_df = read_blob_folder(spark, STORAGE_ACCOUNT, CONTAINER, INPUT_FOLDER)
        
        print("📋 Data schema:")
        original_df.printSchema()
        
        # Cache the DataFrame for better performance during deduplication
        print("💾 Caching data for processing...")
        original_df.cache()
        original_count = original_df.count()
        print(f"📊 Total records loaded: {original_count:,}")
        
        # Perform deduplication
        print("🔄 Performing deduplication...")
        dedup_df = deduplicate_data(original_df, DEDUP_COLUMNS)
        
        # Cache deduplicated DataFrame
        dedup_df.cache()
        
        # Print statistics
        print_dedup_stats(original_df, dedup_df)
        
        # Save deduplicated data
        print("💾 Saving deduplicated data...")
        save_to_blob(dedup_df, STORAGE_ACCOUNT, CONTAINER, OUTPUT_FOLDER, FILE_FORMAT)
        
        print("=" * 60)
        print("✅ DEDUPLICATION PROCESS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        print("Performing cleanup...")
        
    except Exception as e:
        print("=" * 60)
        print("❌ ERROR DURING DEDUPLICATION PROCESS")
        print("=" * 60)
        print(f"Error details: {str(e)}")
        
        # Print additional error context
        import traceback
        print("\nFull error traceback:")
        traceback.print_exc()
        
        # Re-raise the exception after cleanup
        raise
        
    finally:
        # CRITICAL: Always cleanup resources, even on failure
        print("\n" + "=" * 60)
        print("🧹 PERFORMING CLEANUP")
        print("=" * 60)
        
        try:
            # Clean up any temporary directories that might have been created
            if temp_directories:
                cleanup_temp_directories(STORAGE_ACCOUNT, CONTAINER, temp_directories)
            
            # Clean up workspace temp files
            print("Cleaning up workspace temp files...")
            try:
                import glob
                workspace_temp_files = glob.glob("/Workspace/tmp/*")
                for temp_file in workspace_temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception as e:
                        print(f"Warning: Could not remove {temp_file}: {e}")
                if workspace_temp_files:
                    print(f"✓ Cleaned up {len(workspace_temp_files)} workspace temp files")
            except Exception as e:
                print(f"Warning: Error during workspace cleanup: {e}")
            
            # Clean up Spark session (most important)
            # Use proper Databricks environment detection
            is_databricks = is_databricks_environment()
            cleanup_spark_session(spark, is_databricks_session=is_databricks)
            
        except Exception as cleanup_error:
            print(f"⚠️ Error during cleanup: {cleanup_error}")
        
        print("✅ Cleanup completed")

if __name__ == "__main__":
    main()
