import pandas as pd
import awswrangler as wr
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_s3(s3_path: str) -> pd.DataFrame:
    """
    Loads data from a CSV file on S3 using AWS Wrangler.

    Args:
        s3_path: The S3 path (e.g., "s3://your-bucket/data.csv").

    Returns:
        A pandas DataFrame. Returns an empty DataFrame if loading fails.
    """
    logger.info(f"Attempting to load data from: {s3_path}")
    logger.info(f"Using pandas version: {pd.__version__}")
    logger.info(f"Using awswrangler version: {wr.__version__}")
    try:
        # Ensure AWS credentials are configured (environment variables, EC2 role, etc.)
        df = wr.s3.read_csv(path=s3_path)
        logger.info(f"Successfully loaded DataFrame with shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {s3_path}: {e}", exc_info=True)
        # Return an empty DataFrame to allow downstream checks
        return pd.DataFrame()

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame column names: lowercase, replace spaces with underscores.
    """
    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping column name cleaning.")
        return df

    df_cleaned = df.copy()
    df_cleaned.columns = [
        col.strip().lower().replace(' ', '_') for col in df_cleaned.columns
    ]
    logger.info("Cleaned DataFrame column names.")
    return df_cleaned

def add_processing_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with the current timestamp (UTC).
    """
    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping timestamp addition.")
        return df

    df_processed = df.copy()
    # Note: pd.Timestamp.utcnow() was common but deprecated later.
    # pd.Timestamp.now(tz='UTC') is the replacement. Both work in 1.3.5.
    # We use the older one here to show something that *might* need changing later.
    try:
        # This syntax is valid in 1.3.5 but deprecated later
        df_processed['processing_ts_utc'] = pd.Timestamp.utcnow()
    except AttributeError:
        # Fallback for much older pandas if needed, or future-proofing
         df_processed['processing_ts_utc'] = pd.Timestamp.now(tz='UTC')

    # Ensure the timestamp column is of datetime type
    df_processed['processing_ts_utc'] = pd.to_datetime(df_processed['processing_ts_utc'])

    logger.info("Added processing timestamp column.")
    return df_processed

# --- Example of a function that will change significantly in Pandas 2+ ---
# In Pandas 1.3.5, append works but often raises FutureWarning
def append_data(df_original: pd.DataFrame, df_to_append: pd.DataFrame) -> pd.DataFrame:
    """
    Appends one DataFrame to another.
    NOTE: df.append is deprecated in later pandas versions and removed in 2.0.
          Use pd.concat instead for future compatibility.
    """
    logger.warning("Using df.append(), which is deprecated. Consider pd.concat().")
    if df_original.empty:
        return df_to_append.copy()
    if df_to_append.empty:
        return df_original.copy()

    # ignore_index=True is important to reset the index
    combined_df = df_original.append(df_to_append, ignore_index=True)
    return combined_df