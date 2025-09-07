# %%
import dlt
from models.file_table_mapping import file_table_mapping
from models.transformed.facility import Facility
from models.transformed.drugs import Drugs
from models.transformed.forms import Forms
from models.transformed.patients import Patients
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
                        dataset_name="fch_sapphire")
dataset = raw_dataset.dataset()
roster_releases_relation = dataset["roster_releases"]
active_roster_relation = dataset["active_roster"]


# %%
rr_df = roster_releases_relation.df()
ar_df = active_roster_relation.df()
unique_facilities_rr_df = rr_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
unique_facilities_ar_df = ar_df[["facility_id", "facility"]].drop_duplicates() # type: ignore
merged_df = pd.concat([unique_facilities_rr_df, unique_facilities_ar_df]).drop_duplicates() # type: ignore

# %%

@dlt.resource(
    table_name="facility",
    merge_key=["facility_id", "facility_name"],
    columns=Facility,
    schema_contract={
        "data_type": "freeze",
        "tables": "evolve",
        "columns": "freeze"
    },
    write_disposition="merge")
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
def create_table():
    with pipeline.sql_client() as client:
        with client.execute_query(
            '''
            create table if not exists processedData.facility
            (
                facility_id  String,
                facility     String,
                inserted_at  DateTime64(6, 'UTC'),
                _dlt_load_id String,
                _dlt_id      String
            )
                engine = ReplacingMergeTree(inserted_at)
                    PRIMARY KEY facility_id
                    ORDER BY facility_id
                    SETTINGS index_granularity = 8192;

                    SELECT count() FROM system.tables
                    WHERE name = 'facility'
                    AND database = 'processedData';

    '''
        ) as cursor:
            return cursor
        
# %%
a = create_table()
# %%
a.fetchone()
# %%
a.close()
# %%
a
# %%
merged_df.rename(columns={"facility": "facility_name"}, inplace=True)
# %%
merged_df
# %%
merged_df = merged_df[~merged_df["facility_id"].str.match(r"^-+$", na=False)]

# %%
merged_df = merged_df._append({"facility_id": "123", "facility_name": "Test", "inserted_at": datetime.now(timezone.utc)}, ignore_index=True)
# %%
merged_df["inserted_at"] = datetime.now(timezone.utc)
# %%
dataset.schema
# %%
med_pass_results_relation = dataset["med_pass_results"]
active_orders_with_rx_norm_relation = dataset["active_orders_with_rx_norm"]
mpr_df = med_pass_results_relation.df()
aor_df = active_orders_with_rx_norm_relation.df()
mpr_df = mpr_df[["formatted_ndc", "label_name", "alt_name", "strength", "dos_id"]] # type: ignore
aor_df = aor_df[["ndc_code_of_drug_ordered", "drug_name", "drug_strength"]] # type: ignore
aor_df = aor_df[~aor_df["ndc_code_of_drug_ordered"].str.match(r"^-+$", na=False)]
mpr_df = mpr_df[~mpr_df["formatted_ndc"].str.match(r"^-+$", na=False)]
mpr_df["formatted_ndc"] = (
    mpr_df["formatted_ndc"]
    .astype(str)  # ensure string
    .where(lambda x: x.str.isnumeric(), "00000000000")
)
aor_df["ndc_code_of_drug_ordered"] = aor_df["ndc_code_of_drug_ordered"].astype(str).str.replace("-", "", regex=False)
aor_df["ndc_code_of_drug_ordered"] = (
    aor_df["ndc_code_of_drug_ordered"]
    .astype(str)  # ensure string
    .where(lambda x: x.str.isnumeric(), "00000000000")
)
aor_df.drop_duplicates(subset=["ndc_code_of_drug_ordered"], inplace=True)
mpr_df.drop_duplicates(subset=["formatted_ndc"], inplace=True)
mpr_df.rename(
    columns={
        "formatted_ndc": "ndc",
        "label_name": "med_name",
        "alt_name": "med_alt_name",
        "strength": "med_strength",
        "dos_id": "med_form"
        },
    inplace=True
)
aor_df.rename(
    columns={
        "ndc_code_of_drug_ordered": "ndc",
        "drug_name": "med_name",
        "drug_strength": "med_strength"
        },
    inplace=True
)
aor_df["med_alt_name"] = None
aor_df["med_form"] = None
aor_df = aor_df.merge(
    mpr_df[['ndc', 'med_alt_name', 'med_form']], 
    on='ndc', 
    how='left',
    suffixes=('', '_mpr')
)
aor_df["med_alt_name"] = aor_df["med_alt_name_mpr"]
aor_df["med_form"] = aor_df["med_form_mpr"]
aor_df.drop(columns=["med_alt_name_mpr", "med_form_mpr"], inplace=True)
drugs_df = pd.concat([aor_df, mpr_df], ignore_index=True)
drugs_df = drugs_df.drop_duplicates(subset=["ndc"])
drugs_df = drugs_df.where(pd.notna(drugs_df), None)
# %%
@dlt.resource(
    table_name="drugs",
    merge_key=["ndc"],
    columns=Drugs,
    schema_contract={
        "data_type": "freeze",
        "tables": "evolve",
        "columns": "freeze"
    },
    write_disposition="merge")
def drugs_resource():
    records = drugs_df.to_dict(orient="records")
    current_time = datetime.now(timezone.utc)
    for record in records:
        record["inserted_at"] = current_time
    yield records
# %%
pipeline = dlt.pipeline(pipeline_name="drugs", 
                        destination=clickhouse_destination_processed)
pipeline.run(drugs_resource)
# %%
# Form table
form_response_captures_relation = dataset["form_response_captures"]
form_submissions_relation = dataset["form_submissions"]
form_df = form_response_captures_relation.df()
form_df = form_df[["form_name"]] # type: ignore
form_df = form_df.drop_duplicates()
form_df = form_df[~form_df["form_name"].str.match(r"^-+$", na=False)]
#%%
form_df
# %%
import uuid
form_df["form_uuid"] = form_df["form_name"].apply(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)))
# %%
@dlt.resource(
    table_name="forms",
    merge_key=["form_name"],
    columns=Forms,
    schema_contract={
        "data_type": "freeze",
        "tables": "evolve",
        "columns": "freeze"
    },
    write_disposition="merge")
def forms_resource():
    records = form_df.to_dict(orient="records")
    current_time = datetime.now(timezone.utc)
    for record in records:
        record["inserted_at"] = current_time
    yield records
# %%
pipeline = dlt.pipeline(pipeline_name="forms", 
                        destination=clickhouse_destination_processed)
pipeline.run(forms_resource)
# %%
# patients table
dataset.schema
# %%
releases_relation = dataset["roster_releases"]
active_roster_relation = dataset["active_roster"]
releases_df = releases_relation.df()
active_roster_df = active_roster_relation.df()
active_roster_df = active_roster_df[["sapphire_pat_id", "patient_name", "dob", "gender"]] # type: ignore
releases_df = releases_df[["sapphire_pat_id", "patient_name", "dob", "gender"]] # type: ignore
active_roster_df = active_roster_df[~active_roster_df["sapphire_pat_id"].str.match(r"^-+$", na=False)]
releases_df = releases_df[~releases_df["sapphire_pat_id"].str.match(r"^-+$", na=False)]
active_roster_df = active_roster_df.drop_duplicates(subset=["sapphire_pat_id"])
releases_df = releases_df.drop_duplicates(subset=["sapphire_pat_id"])
active_roster_df = active_roster_df.dropna(subset=["sapphire_pat_id"])
releases_df = releases_df.dropna(subset=["sapphire_pat_id"])
patients_df = pd.concat([active_roster_df, releases_df], ignore_index=True)
patients_df = patients_df.drop_duplicates(subset=["sapphire_pat_id"], keep="first")
patients_df.rename(columns={"sapphire_pat_id": "mrn"}, inplace=True)
patients_df[["last_name", "first_name"]] = patients_df["patient_name"].str.split(",", n=1, expand=True)
patients_df["first_name"] = patients_df["first_name"].str.strip()
patients_df["last_name"] = patients_df["last_name"].str.strip()
patients_df.drop(columns=["patient_name"], inplace=True)
# %%
patients_df

# %%
@dlt.resource(
    table_name="patients",
    merge_key=["mrn"],
    columns=Patients,
    schema_contract={
        "data_type": "freeze",
        "tables": "evolve",
        "columns": "freeze"
    },
    write_disposition="merge")
def patients_resource():
    records = patients_df.to_dict(orient="records")
    current_time = datetime.now(timezone.utc)
    for record in records:
        record["inserted_at"] = current_time
    yield records
# %%
pipeline = dlt.pipeline(pipeline_name="patients", 
                        destination=clickhouse_destination_processed)
pipeline.run(patients_resource)
# %%
