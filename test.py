# %%
import dlt
from models.file_table_mapping import file_table_mapping
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource
from dltConfig.resources import DLTResource
from dltConfig.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import shutil
import glob
load_dotenv()

def rename_columns(header_mapping):
    """
    Generic function to rename dictionary keys based on a header mapping.
    
    Args:
        header_mapping (dict): Dictionary mapping old column names to new column names
    
    Returns:
        function: A function that can be used with add_map() to rename columns
    """
    def rename_dict_keys(d):
        result = {}
        for k, v in d.items():
            new_key = header_mapping.get(k, k)  # Use mapped name or keep original
            result[new_key] = v
        return result
    return rename_dict_keys

# %%
header_to_columns = {
    "FACILITY_ID": "facility_id",
    "UNIT": "unit",
    "NPI Number of ordering Practitioner": "npi_number_of_ordering_practitioner",
    "Name of Ordering Provider": "name_of_ordering_provider",
    "Date & Time order Received": "date_time_order_received",
    "NDC Code of Drug ordered": "ndc_code_of_drug_ordered",
    "Drug Name": "drug_name",
    "Effective Date": "effective_date",
    "Dosage": "dosage",
    "Drug Strength": "drug_strength",
    "Frequency": "frequency",
    "Duration": "duration",
    "Route": "route",
    "Number of Refills Issued": "number_of_refills_issued",
    "Expiration Date": "expiration_date",
    "Provider Admin Instructions": "provider_admin_instructions",
    "Dispensed Quantity": "dispensed_quantity",
    "Formulary Or Non Formulary Drug Flag": "formulary_or_non_formulary_drug_flag",
    "Keep On Person Indicator": "keep_on_person_indicator",
    "What time of day the medication should be taken at (AM, PM, NOON, BEDTIME)": "medication_time_of_day",
    "ORDER_ID": "order_id",
    "ORDERED_BY": "ordered_by"
}

# %%
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="clickhouse")
files = filesystem(bucket_url="bucket/incoming",file_glob="incoming_20250818_ActiveORDERSWithRXNorm.csv") | read_csv()
files = files.add_map(rename_columns(header_to_columns))

# %%
for file in files:
    print(file)
    break

# %%
clickhouse_destination = dlt.destinations.clickhouse()
pipeline = dlt.pipeline(pipeline_name="data_transformation", 
                        destination=clickhouse_destination,
                        dataset_name="fch_analytics")

# %%
dataset = pipeline.dataset()
# %%
roster_releases_relation = dataset["roster_releases"]
active_roster_relation = dataset["active_roster"]
# %%
rr_df = roster_releases_relation.df()
# %%
ar_df = active_roster_relation.df()
# %%
unique_facilities_rr_df = rr_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
unique_facilities_ar_df = ar_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
# %%
unique_facilities_rr_df.head(100000000)
# %%
unique_facilities_ar_df.head(100000000)
# %%
import pandas as pd # type: ignore
# %%
merged_df = pd.concat([unique_facilities_rr_df, unique_facilities_ar_df]).drop_duplicates() # type: ignore
# %%
merged_df.head(100000000)
# %%
# will have a patient table, and then two tables active and released on top of that