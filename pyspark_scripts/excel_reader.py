
import logging
import pandas as pd
import re
from pyspark.sql import SparkSession, DataFrame
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CQDataIngestion")

def _normalize_column_name(col_name: str) -> str:
    """
    Normalizes a column name by converting to lowercase, replacing special characters with underscores,
    and removing leading/trailing underscores.
    """
    if not isinstance(col_name, str):
        col_name = str(col_name)
    col_name = col_name.lower().strip()
    # Replace non-alphanumeric characters with a single underscore
    col_name = re.sub(r'[^a-z0-9]+', '_', col_name)
    # Remove leading/trailing underscores that might have been created
    col_name = col_name.strip('_')
    return col_name

def read_excel(abfss_path: str, sheet_name=0) -> DataFrame | None:
    """
    Reads an Excel file from an ABFSS path, normalizes column names, and returns a Spark DataFrame.
    This function requires the 'openpyxl' library to be installed to read .xlsx files.

    Args:
        abfss_path (str): The full ABFSS path to the Excel file (e.g., 'abfss://container@storageaccount.dfs.core.windows.net/path/to/file.xlsx').
        sheet_name (str or int, optional): The name or index of the sheet to read. Defaults to 0 (the first sheet).

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the data from the Excel file and normalized column names.
        Returns None if an error occurs.
    """
    try:
        logger.info(f"Starting to read Excel file from: {abfss_path}")

        # Get the active Spark session
        spark = SparkSession.builder.getOrCreate()

        # Spark can read the binary content of the file. This avoids issues with DBFS mounts.
        # This reads the entire file into the driver's memory, so it's suitable for moderately sized Excel files.
        logger.info("Reading file content as binary.")
        file_content = spark.read.format("binaryFile").load(abfss_path).collect()[0]['content']

        # Read the binary content into a pandas DataFrame
        logger.info("Parsing Excel data using pandas.")
        pandas_df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, engine='openpyxl')

        # Normalize column names
        logger.info("Normalizing column names.")
        normalized_columns = {col: _normalize_column_name(col) for col in pandas_df.columns}
        pandas_df = pandas_df.rename(columns=normalized_columns)
        logger.info(f"Normalized column names: {list(pandas_df.columns)}")

        # Convert pandas DataFrame to Spark DataFrame
        logger.info("Converting pandas DataFrame to Spark DataFrame.")
        spark_df = spark.createDataFrame(pandas_df)

        logger.info(f"Successfully created Spark DataFrame from {abfss_path}")
        return spark_df

    except Exception as e:
        logger.error(f"An error occurred while processing {abfss_path}: {e}", exc_info=True)
        # Depending on requirements, you might want to raise the exception,
        # return an empty DataFrame, or return None.
        return None
