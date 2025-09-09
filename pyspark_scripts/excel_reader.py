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


def _make_unique_columns(cols):
    """Append suffixes to duplicate column names to make them unique while keeping a readable form."""
    counts = Counter()
    unique = []
    for c in cols:
        counts[c] += 1
        if counts[c] == 1:
            unique.append(c)
        else:
            unique.append(f"{c}__dup{counts[c]-1}")
    return unique


def read_excel(abfss_path: str, sheet_name=0) -> "pyspark.sql.DataFrame | None":
    """
    Read an .xlsx from ABFSS into a Spark DataFrame (all-string schema).
    Assumes helper functions `_normalize_column_name(name: str) -> str`
    and `_make_unique_columns(cols: list[str]) -> list[str]` are available.
    """
    try:
        logger.info(f"Starting to read Excel file from: {abfss_path}")
        spark = SparkSession.builder.getOrCreate()

        # read binaryFile and pick first matching file
        logger.info("Reading file content as binary.")
        bin_df = spark.read.format("binaryFile").load(abfss_path)
        if bin_df.count() == 0:
            logger.error(f"No files found at {abfss_path}")
            return None
        row = bin_df.select("content", "path").limit(1).collect()[0]
        file_content = row["content"]
        actual_path = row["path"]
        if file_content is None:
            logger.error("binaryFile returned empty content.")
            return None

        # parse into pandas
        logger.info("Parsing Excel data using pandas.")
        pandas_df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, engine="openpyxl")

        # --- normalize column names (use existing helpers) ---
        logger.info("Normalizing column names.")
        orig_cols = list(pandas_df.columns)
        normalized = [_normalize_column_name(c) for c in orig_cols]        # assumes helper exists
        unique_cols = _make_unique_columns(normalized)                   # assumes helper exists

        if unique_cols != normalized:
            logger.warning("Found duplicate column names after normalization. Added suffixes to make them unique.")
            for orig, norm, uniq in zip(orig_cols, normalized, unique_cols):
                if norm != uniq:
                    logger.debug(f"  {orig!r} -> {uniq!r}")

        # assign unique names back (DO NOT call rename with a list)
        pandas_df.columns = unique_cols
        logger.debug(f"Final column names ({len(pandas_df.columns)}): {pandas_df.columns.tolist()[:50]}")

        # Force all values to string and restore None for missing
        pandas_df = pandas_df.astype(str).where(pandas_df.notna(), None)

        # explicit all-string schema to avoid Spark inference issues
        schema = StructType([StructField(str(c), StringType(), True) for c in pandas_df.columns])

        # convert to records and make Spark DF
        records = pandas_df.to_dict(orient="records")
        logger.info("Converting pandas DataFrame to Spark DataFrame.")
        spark_df = spark.createDataFrame(records, schema=schema)

        # attach metadata
        spark_df = spark_df.withColumn("_metadata", struct(lit(actual_path).alias("file_path")))

        logger.info(f"Successfully created Spark DataFrame from {abfss_path}")
        return spark_df

    except Exception as e:
        logger.error(f"An error occurred while processing {abfss_path}: {e}", exc_info=True)
        return None

