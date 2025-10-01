# run_pipeline.py
import pandas as pd
import great_expectations as gx
import os
from datetime import date

# --- Configuration (Must Match Phase 2 Setup) ---
CHECKPOINT_NAME = "my_etl_quality_gate" 
DATASOURCE_NAME = "my_filesystem_data_source"
SUITE_NAME = "raw_data_contract"

# Output configuration
OUTPUT_DIR = "features" 
TODAY_STR = date.today().isoformat()
# Partitioned Parquet path (e.g., features/date=2025-10-01/final_features.parquet)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"date={TODAY_STR}", "final_features.parquet")

def run_quality_gate(df_raw: pd.DataFrame, input_file_name: str) -> bool:
    """
    Runs the Great Expectations Checkpoint on the raw data.
    Stops the pipeline if the data quality fails.
    """
    print(f"\n--- 1. Running Quality Gate for: {input_file_name} ---")
    
    # 1. Load the configured context and Checkpoint
    context = gx.get_context()
    checkpoint = context.checkpoints.get(CHECKPOINT_NAME)

    # 2. Define the Runtime Batch Request for the Checkpoint
    # This structure is vital for passing the in-memory DataFrame (df_raw)
    runtime_batch_request = {
        "datasource_name": DATASOURCE_NAME,
        "data_connector_name": "default_runtime_data_connector", 
        "data_asset_name": "runtime_asset", # Matches name used in the Checkpoint config
        "runtime_parameters": {"batch_data": df_raw},
        "batch_identifiers": {"default_identifier_name": input_file_name},
    }
    checkpoint.run()
    batch_parameters = {"month": "01", "year": "2019", "day":"15"}
    validation_results = checkpoint.run(batch_parameters=batch_parameters)
    # 3. Run the Checkpoint
    # checkpoint_result = checkpoint.run(
    #     run_name=f"validation_{input_file_name}_{TODAY_STR}",
    #     validations=[
    #         {
    #             "batch_request": runtime_batch_request,
    #             "expectation_suite_name": SUITE_NAME,
    #         }
    #     ]
    # )

    # 4. Decision Logic
    validation_success = validation_results.success
    # print(validation_results.success)
    
    print(f"Quality Gate Result: {'PASS' if validation_success else 'FAIL'}!")
    
    if not validation_results.success:
        print("🛑 Data contract failed! Stopping ETL to block corrupt data.")
        # The SlackNotificationAction defined in Phase 2 would trigger here.
        return False
        
    print("✅ Data quality passed. Proceeding to transformation.")
    return True


def transform_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Performs the feature transformation if the quality check passes."""
    print("\n--- 2. Performing Transformation (Pandas) ---")
    
    # Transformation Example: Calculate new features aggregated by patient
    df_transformed = df_raw.groupby('patient_ID').agg(
        Avg_Heart_Rate=('Heart_Rate', 'mean'),
        Max_Anxiety_Score=('Anxiety_Score', 'max'),
        # Simulate a complex derived feature
        Avg_Weekly_HRV=('Heart_Rate', lambda x: 80 - x.mean()), 
        Latest_Depression_Score=('Depression_Score', 'last')
    ).reset_index()

    return df_transformed


def load_data(df_final: pd.DataFrame):
    """Writes the final, certified features to a date-partitioned Parquet file."""
    print(f"\n--- 3. Loading Final Certified Features (Parquet) ---")
    
    # Ensure the date-partitioned directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True) 

    # Write the DataFrame to Parquet (required format for model training)
    df_final.to_parquet(OUTPUT_PATH, index=False)
    
    print(f"✨ Successfully wrote certified features to: {OUTPUT_PATH}")

    
def main(input_file: str):
    """Main pipeline execution function."""
    
    # Extract: Load the raw data
    try:
        df_raw = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file}")
        return

    # Quality Gate: Run the Checkpoint
    if run_quality_gate(df_raw, input_file):
        
        # Transform: If the gate passed
        df_final = transform_data(df_raw)
        
        # Load: Write the final Parquet
        load_data(df_final)
        
    print("\nPipeline End.")


if __name__ == "__main__":
    # Test 1: Corrupt Data (Should FAIL to test the Quality Gate)
    CORRUPT_FILE = os.path.join("data", "raw_data_day_2019-01-15.csv") 
    main(CORRUPT_FILE) 

    # To run the success test, change the line above to:
    # CLEAN_FILE = os.path.join("data", "raw_data_day_1.csv") 
    # main(CLEAN_FILE)