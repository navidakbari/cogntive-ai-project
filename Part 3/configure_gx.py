#%%
# Import core libraries
import great_expectations as gx
import pandas as pd
import os
from great_expectations.data_context.data_context.file_data_context import FileDataContext

# Define Constants
GX_PROJECT_DIR = "gx"

print(f"GX Version: {gx.__version__}")

# Initialize or Load the Data Context
# This creates the 'gx/' directory if it doesn't exist.
if not os.path.isdir(GX_PROJECT_DIR):
    print("Initializing new FileDataContext...")
    context = FileDataContext(project_root_dir=os.path.join(os.getcwd()))
else:
    print("Loading existing FileDataContext...")
    context = gx.get_context(context_root_dir=os.path.join(os.getcwd()))
    
print(f"\nContext Status: {type(context).__name__}")
print(f"Context Directory: {context.root_directory}")

#%%
# 2. Define the Datasource (Source: Folder, Engine: Pandas)

DATASOURCE_NAME = "my_filesystem_data_source"
DATA_DIR = "data"

try:
    # Use add_pandas_filesystem to configure the root folder for Pandas reading
    data_source = context.data_sources.add_pandas_filesystem(
        name=DATASOURCE_NAME, 
        base_directory=DATA_DIR
    )
    print(f"Datasource '{DATASOURCE_NAME}' configured.")
except Exception:
    data_source = context.data_sources.get(DATASOURCE_NAME)
    print(f"Datasource '{DATASOURCE_NAME}' retrieved.")

# Save the config to disk
context.variables.save()

#%%
# 3. Define the Data Asset (Fluent API FIX)
ASSET_NAME = "raw_data_day_asset" 

try:
    # FIX: Add the 'pattern' argument to tell GX how to discover files.
    data_asset = data_source.add_csv_asset(name=ASSET_NAME)
    print(f"Asset '{ASSET_NAME}' created successfully with file discovery pattern.")
except Exception:
    data_asset = data_source.get_asset(ASSET_NAME)
    print(f"Asset '{ASSET_NAME}' retrieved (assumed present).")

# Optional: Print the asset to confirm it's configured
print(data_asset)
context.variables.save()

#%%

batch_definition_name = "daily_raw_data"
batch_definition_regex = r"raw_data_day_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv"
try:
    batch_definition = data_asset.add_batch_definition_daily(
        name=batch_definition_name, regex=batch_definition_regex
    )
except:
    print("Batch already exists")
batch = batch_definition.get_batch(batch_parameters={"year": "2019", "month": "01", "day": "15"})
print(batch.head())

# %%

SUITE_NAME = "raw_data_contract"
print(f"--- Defining Expectation Suite: {SUITE_NAME} ---")

suite = gx.ExpectationSuite(name=SUITE_NAME)
suite = context.suites.add(suite)

#%%


# CLEAN_DATA_PATH = os.path.join("data", "raw_data_day_2019-01-15.csv")
# clean_df = pd.read_csv(CLEAN_DATA_PATH) # Note: Using 'pandas' as per the documentation


# batch_parameters = {"year": clean_df}
batch_parameters = {"year": "2019", "month": "01", "day": "15"}

batch_definition = (
    context.data_sources.get(DATASOURCE_NAME)
    .get_asset(ASSET_NAME)
    .get_batch_definition(batch_definition_name)
)

batch = batch_definition.get_batch(batch_parameters=batch_parameters)

#%%
validator = batch.validate(suite)
print(f"Validator initialized with clean data, ready for rule definition.")


#%%

suite.add_expectation(gx.expectations.ExpectColumnToExist(column="Age"))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column="Depression_Score"))

# 2. Critical Null Check (Will fail on corrupt data if Heart_Rate has NaNs)
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="Heart_Rate")) 
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="patient_ID")) 

# 3. Outlier Check (Will fail on corrupt data: Sleep_Hours=100)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="Sleep_Hours", 
        min_value=3.0, 
        max_value=15.0, 
        mostly=1.0 # Requires 100% compliance
    )
) 

# 4. Data Drift Check
suite.add_expectation(
    gx.expectations.ExpectColumnMeanToBeBetween(
        column="Anxiety_Score", 
        min_value=15.0, 
        max_value=25.0
    )
)
#%%
# Save the updated Expectation Suite to the filesystem
context.suites.add_or_update(suite)
print(f"\nExpectation Suite '{SUITE_NAME}' saved with all critical expectations.")

#%%
expectation_suite = context.suites.get(name=SUITE_NAME)

# 2. Retrieve the Batch Definition object
batch_definition = (
    context.data_sources.get(DATASOURCE_NAME)
    .get_asset(ASSET_NAME)
    .get_batch_definition(batch_definition_name)
)

batch_definition.save()

# 3. Create the Validation Definition object
# This binds the data source (BatchDefinition) to the rules (ExpectationSuite)
definition_name = "validation_definition"

# FIX: We use the BatchDefinition object and the ExpectationSuite object directly.
validation_definition = gx.ValidationDefinition(
    data=batch_definition, 
    suite=expectation_suite, 
    name=definition_name
)
validation_definition = context.validation_definitions.add(validation_definition)


print(f"Validation Definition '{definition_name}' created in memory.")
#%%

from great_expectations.checkpoint.actions import (
    UpdateDataDocsAction
)

CHECKPOINT_NAME = "my_etl_quality_gate"
print(f"--- Creating Checkpoint: {CHECKPOINT_NAME} ---")

action_list = [
    # Update Data Docs
    UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]

validation_definitions = [
    context.validation_definitions.get(definition_name)
]

checkpoint = gx.Checkpoint(
    name=CHECKPOINT_NAME,
    validation_definitions=validation_definitions,
    actions=action_list,
    result_format={"result_format": "COMPLETE"},
)
# Build the documentation site so you can view the results later
context.checkpoints.add(checkpoint)


print("✨ Checkpoint defined and Data Docs built. Phase 2 Complete.")
# %%

context.build_data_docs()
# %%
