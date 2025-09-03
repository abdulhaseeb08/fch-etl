# %%
import dlt
from models.file_table_mapping import file_table_mapping
from models.transformed.facility import Facility
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource
from dltConfig.resources import DLTResource
from dltConfig.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed
from dotenv import load_dotenv
import pandas as pd # type: ignore
import os
from datetime import datetime, timezone
import shutil
import glob
load_dotenv()
clickhouse_destination_raw = dlt.destinations.clickhouse(credentials={
    "database": "rawData",
    "username": "default",
    "password": "august",
    "host": "localhost",
    "http_port": 8123,
    "port": 9000,
    "secure": 0
})
clickhouse_destination_processed = dlt.destinations.clickhouse(credentials={
    "database": "processedData",
    "username": "default",
    "password": "august",
    "host": "localhost",
    "http_port": 8123,
    "port": 9000,
    "secure": 0
})
raw_dataset = dlt.pipeline(pipeline_name="data_transformation", 
                        destination=clickhouse_destination_raw,
                        dataset_name="fch_analytics")
dataset = raw_dataset.dataset()
roster_releases_relation = dataset["roster_releases"]
active_roster_relation = dataset["active_roster"]
rr_df = roster_releases_relation.df()
ar_df = active_roster_relation.df()
unique_facilities_rr_df = rr_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
unique_facilities_ar_df = ar_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
merged_df = pd.concat([unique_facilities_rr_df, unique_facilities_ar_df]).drop_duplicates() # type: ignore
@dlt.resource(
    table_name="facility",
    columns=Facility,
    schema_contract={
        "data_type": "freeze",
        "tables": "evolve",
        "columns": "freeze"
    },
    write_disposition="append")
def facility_resource():
    records = merged_df.to_dict(orient="records")
    current_time = datetime.now(timezone.utc)
    for record in records:
        record["inserted_at"] = current_time
    yield records
# %%
pipeline = dlt.pipeline(pipeline_name="facility", 
                        destination=clickhouse_destination_processed)
pipeline.run(facility_resource)
# %%
merged_df.to_dict(orient="records")
# %%
