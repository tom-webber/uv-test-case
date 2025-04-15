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
    # Note: pd.Timestamp.utcnow() was deprecated and removed in pandas 2.0
    # Using the recommended pd.Timestamp.now(tz='UTC') approach
    df_processed['processing_ts_utc'] = pd.Timestamp.now(tz='UTC')

    # Ensure the timestamp column is of datetime type
    df_processed['processing_ts_utc'] = pd.to_datetime(df_processed['processing_ts_utc'])

    logger.info("Added processing timestamp column.")
    return df_processed

# --- Function updated for pandas 2.0+ compatibility ---
def append_data(df_original: pd.DataFrame, df_to_append: pd.DataFrame) -> pd.DataFrame:
    """
    Combines two DataFrames.
    Uses pd.concat which is the recommended approach in pandas 2.0+
    (df.append was removed in pandas 2.0)
    """
    logger.info("Using pd.concat to combine DataFrames")
    if df_original.empty:
        return df_to_append.copy()
    if df_to_append.empty:
        return df_original.copy()

    # Use pd.concat instead of append
    combined_df = pd.concat([df_original, df_to_append], ignore_index=True)
    return combined_df