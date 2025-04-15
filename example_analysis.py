import pandas as pd
from src.my_tooling_lib import (
    load_data_from_s3,
    clean_column_names,
    add_processing_timestamp,
    append_data
)
import os
import logging # Use the same logger setup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# IMPORTANT: Replace with a real S3 path you have access to
# Ensure your AWS credentials are configured (e.g., via environment variables,
# AWS config file, or IAM role if running on EC2/ECS/Lambda)
# S3_DATA_PATH = "s3://your-actual-bucket-name/your-data.csv"
if __name__ == "__main__":
    # --- Use a Local Dummy File for Demo if S3 isn't available ---
    # Create dummy data if it doesn't exist
    DUMMY_CSV_PATH = "dummy_data.csv"
    if not os.path.exists(DUMMY_CSV_PATH):
        logger.info(f"Creating dummy data file: {DUMMY_CSV_PATH}")
        dummy_df = pd.DataFrame({
            'ID': [1, 2, 3],
            'User Name': ['Alice Smith', ' Bob Jones ', 'charlie     '],
            ' Engagement Score ': [85.5, 92.1, 78.0]
        })
        dummy_df.to_csv(DUMMY_CSV_PATH, index=False)
        # For local demo, we'll use pandas directly instead of S3 loader
        USE_LOCAL_FILE = True
        LOCAL_DATA_PATH = DUMMY_CSV_PATH
    else:
        USE_LOCAL_FILE = True
        LOCAL_DATA_PATH = DUMMY_CSV_PATH
        # If you want to force S3 usage, set USE_LOCAL_FILE = False and ensure S3_DATA_PATH is correct

    # --- Main Analysis Script ---
    logger.info("--- Starting Example Analysis ---")
    logger.info(f"Using pandas version: {pd.__version__}") # Verify version

    # Load Data
    if USE_LOCAL_FILE:
        logger.info(f"Loading data locally from: {LOCAL_DATA_PATH}")
        try:
            initial_df = pd.read_csv(LOCAL_DATA_PATH)
            logger.info(f"Local load successful. Shape: {initial_df.shape}")
        except Exception as e:
            logger.error(f"Failed to load local file: {e}")
            initial_df = pd.DataFrame()
    else:
        # This requires AWS credentials and the s3 path to be valid
        logger.info(f"Loading data from S3: {S3_DATA_PATH}")
        initial_df = load_data_from_s3(S3_DATA_PATH)

    # Proceed only if data loading was successful
    if not initial_df.empty:
        # Process Data using library functions
        cleaned_df = clean_column_names(initial_df)
        processed_df = add_processing_timestamp(cleaned_df)

        logger.info("\n--- Processed DataFrame Head ---")
        logger.info(f"\n{processed_df.head().to_string()}") # Use to_string for better console output

        # Example: Calculate average score
        if 'engagement_score' in processed_df.columns:
            avg_score = processed_df['engagement_score'].mean()
            logger.info(f"\nAverage Engagement Score: {avg_score:.2f}")
        else:
            logger.warning("Column 'engagement_score' not found after cleaning.")

        # Demonstrate the append function (which uses deprecated method)
        logger.info("\n--- Demonstrating Append ---")
        new_data = pd.DataFrame({
            'id': [4],
            'user_name': ['David Lee'],
            'engagement_score': [88.8]
            # Note: Column names match the *cleaned* DataFrame
        })
        combined_df = append_data(processed_df, new_data)
        logger.info("Appended new data:")
        logger.info(f"\n{combined_df.tail().to_string()}")

    else:
        logger.error("Data loading failed. Cannot proceed with analysis.")

    logger.info("\n--- Analysis Complete ---")

    # --- Cleanup Dummy Data (Optional) ---
    # if USE_LOCAL_FILE and os.path.exists(DUMMY_CSV_PATH):
    #     logger.info(f"Cleaning up dummy data file: {DUMMY_CSV_PATH}")
    #     os.remove(DUMMY_CSV_PATH)