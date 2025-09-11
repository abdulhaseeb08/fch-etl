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
form_response_captures_relation = dataset["form_response_captures"]
frc_df = form_response_captures_relation.df()
# %%
frc_df = frc_df[["sapphire_pat_id", "facility_id", "form_name", "cap_desc", "ans_text", "date_created"]] # type: ignore
frc_df = frc_df[~frc_df["sapphire_pat_id"].str.match(r"^-+$", na=False)]
# %%
frc_df
# %%
# group and build JSON column
json_df = (
    frc_df
    .groupby(["sapphire_pat_id", "facility_id", "form_name", "date_created"])
    .apply(lambda g: g.set_index("cap_desc")["ans_text"].to_dict(), include_groups=False)
    .reset_index(name="form_data")
)
json_df.rename(columns={"sapphire_pat_id": "mrn"}, inplace=True)

# %%
import uuid
json_df["form_uuid"] = json_df["form_name"].apply(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)))

# %
# %%
json_df.drop(columns=["form_name"], inplace=True)
# %%


# %%
json_df.dtypes
# %%
ar_relation = dataset["active_roster"]
ar_df = ar_relation.df() # type: ignore
ar_df = ar_df[~ar_df["sapphire_pat_id"].str.match(r"^-+$", na=False)] # type: ignore
ar_df["file_date"] = ar_df["file_name"].str.extract(r"_(\d{8})_") # type: ignore
ar_df["file_date"] = pd.to_datetime(ar_df["file_date"], format="%Y%m%d") # type: ignore
max_date = ar_df["file_date"].max() # type: ignore
ar_df = ar_df[ar_df["file_date"] == max_date] # type: ignore
validation_failed = []
dupe_df = ar_df[ar_df.duplicated(subset=["sapphire_pat_id"], keep=False)] # send mail if this exists 
if not dupe_df.empty:
    temp = dupe_df.copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
    temp["reason"] = "duplicate sapphire_pat_id"
    validation_failed.append(temp)
invalid_booked_date = ar_df["last_booked_date"].notna() & ~pd.to_datetime(ar_df["last_booked_date"], errors="coerce").notna()
if invalid_booked_date.any():
    temp = ar_df[invalid_booked_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
    temp["reason"] = "last_booked_date is not a datetime"
    validation_failed.append(temp)
invalid_release_date = ar_df["last_release_date"].notna() & ~pd.to_datetime(ar_df["last_release_date"], errors="coerce").notna()
if invalid_release_date.any():
    temp = ar_df[invalid_release_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
    temp["reason"] = "last_release_date is not a datetime"
    validation_failed.append(temp)
invalid_absent = ~ar_df["ata_status"].isin(["Y", "N"])
if invalid_absent.any():
    temp = ar_df[invalid_absent].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
    temp["reason"] = "ata_status must be 'Y' or 'N'"
    validation_failed.append(temp)
absent_facility = ar_df["facility_id"].isna() | ar_df["facility"].isna() | ar_df["facility"].str.strip() == ""
if absent_facility.any():
    temp = ar_df[absent_facility].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
    temp["reason"] = "facility_id or facility is None or Nan or empty"
    validation_failed.append(temp)

if validation_failed:
    validation_failed_df = pd.concat(validation_failed, ignore_index=True)
    validation_failed_df.rename(columns={
        "sapphire_pat_id": "SAPPHIRE_PAT_ID",
        "facility": "FACILITY",
        "facility_id": "FACILITY_ID",
        "jms_id": "JMS_ID",
        "patient_name": "PATIENT_NAME",
        "unit": "UNIT",
        "wing": "WING",
        "room": "ROOM",
        "bed": "BED",
        "dob": "DOB",
        "gender": "GENDER",
        "last_booked_date": "LAST_BOOKED_DATE",
        "last_release_date": "LAST_RELEASE_DATE",
        "ata_status": "ATA_STATUS",
    })
else:
    validation_failed_df = pd.DataFrame(columns=list(ar_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id", "file_name", "file_date"], inplace=True)
ar_df = ar_df[~ar_df["sapphire_pat_id"].isin(validation_failed_df["sapphire_pat_id"])]
ar_df["ata_status"] = ar_df["ata_status"].map({"N": False, "Y": True})
ar_df = ar_df[["sapphire_pat_id", "facility_id", "jms_id", "unit", "wing", "room", "bed", "last_booked_date", "last_release_date", "ata_status"]] # type: ignore
ar_df.rename(columns={"sapphire_pat_id": "mrn", "unit": "loc_unit", "wing": "loc_wing", "room": "loc_room", "bed": "loc_bed", "ata_status": "is_absent"}, inplace=True)
# %%
ar_df
# %%
validation_failed_df
# %%
# %%
rr_relation = dataset["roster_releases"]
rr_df = rr_relation.df()
rr_df = rr_df[~rr_df["sapphire_pat_id"].str.match(r"^-+$", na=False)] # type: ignore
dupe_df = rr_df[rr_df.duplicated(subset=["sapphire_pat_id"], keep=False)] # send mail if this exists 
if not dupe_df.empty:
    temp = dupe_df.copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "duplicate sapphire_pat_id"
    validation_failed.append(temp)
invalid_booked_date = rr_df["last_booked_date"].isna() | (rr_df["last_booked_date"].notna() & ~pd.to_datetime(rr_df["last_booked_date"], errors="coerce").notna())
if invalid_booked_date.any():
    temp = rr_df[invalid_booked_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "last_booked_date is not a datetime"
    validation_failed.append(temp)
invalid_release_date = rr_df["last_release_date"].isna() | (rr_df["last_release_date"].notna() & ~pd.to_datetime(rr_df["last_release_date"], errors="coerce").notna())
if invalid_release_date.any():
    temp = rr_df[invalid_release_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "last_release_date is NULL or not a datetime"
    validation_failed.append(temp)
absent_facility = rr_df["facility_id"].isna() | rr_df["facility"].isna() | rr_df["facility"].str.strip() == ""
if absent_facility.any():
    temp = rr_df[absent_facility].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "facility_id or facility is None or Nan or empty"
    validation_failed.append(temp)
absent_sapphire_pat_id = rr_df["sapphire_pat_id"].isna() | rr_df["sapphire_pat_id"].str.strip() == ""
if absent_sapphire_pat_id.any():
    temp = rr_df[absent_sapphire_pat_id].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "sapphire_pat_id is NULL"
    validation_failed.append(temp)

if validation_failed:
    validation_failed_df = pd.concat(validation_failed, ignore_index=True)
    validation_failed_df.rename(columns={
        "sapphire_pat_id": "SAPPHIRE_PAT_ID",
        "facility": "FACILITY",
        "facility_id": "FACILITY_ID",
        "jms_id": "JMS_ID",
        "patient_name": "PATIENT_NAME",
        "unit": "UNIT",
        "wing": "WING",
        "room": "ROOM",
        "bed": "BED",
        "dob": "DOB",
        "gender": "GENDER",
        "last_booked_date": "LAST_BOOKED_DATE",
        "last_release_date": "LAST_RELEASE_DATE",
    })
else:
    validation_failed_df = pd.DataFrame(columns=list(rr_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
rr_df = rr_df[~rr_df["sapphire_pat_id"].isin(validation_failed_df["sapphire_pat_id"])]
rr_df = rr_df[["sapphire_pat_id", "facility_id", "jms_id", "unit", "wing", "room", "bed", "last_booked_date", "last_release_date"]] # type: ignore
rr_df.rename(columns={"sapphire_pat_id": "mrn", "unit": "loc_unit", "wing": "loc_wing", "room": "loc_room", "bed": "loc_bed"}, inplace=True)
# %%
rr_df
# %%
validation_failed = []
mpl_relation = dataset["med_pass_results"]
mpl_df = mpl_relation.df()
mpl_df = mpl_df[~mpl_df["sapphire_pat_id"].str.match(r"^-+$", na=False)] # type: ignore
absent_facility = mpl_df["facility_id"].isna() | (mpl_df["facility_id"].str.strip() == "")
if absent_facility.any():
    temp = mpl_df[absent_facility].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "FACILITY_ID is None or Nan or empty"
    validation_failed.append(temp)
absent_sapphire_pat_id = mpl_df["sapphire_pat_id"].isna() | (mpl_df["sapphire_pat_id"].str.strip() == "")
if absent_sapphire_pat_id.any():
    temp = mpl_df[absent_sapphire_pat_id].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "SAPPHIRE_PAT_ID is NULL"
    validation_failed.append(temp)
absent_formatted_ndc = mpl_df["formatted_ndc"].isna() | (mpl_df["formatted_ndc"].str.strip() == "") | (~mpl_df["formatted_ndc"].str.isnumeric())
if absent_formatted_ndc.any():
    temp = mpl_df[absent_formatted_ndc].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "FORMATTED_NDC is NULL or empty or non numeric"
    validation_failed.append(temp)
absent_result_date = mpl_df["result_date"].notna() & (~pd.to_datetime(mpl_df["result_date"], errors="coerce").notna())
if absent_result_date.any():
    temp = mpl_df[absent_result_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "RESULT_DATE is not a datetime"
    validation_failed.append(temp)
absent_medpass_date = mpl_df["medpass_date"].notna() & (~pd.to_datetime(mpl_df["medpass_date"], errors="coerce").notna())
if absent_medpass_date.any():
    temp = mpl_df[absent_medpass_date].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "MEDPASS_DATE is not a datetime"
    validation_failed.append(temp)
absent_medpass_date_not_missed = mpl_df["medpass_date"].isna() & (mpl_df["medpass_result"].astype(str).str.lower() != "missed")
if absent_medpass_date_not_missed.any():
    temp = mpl_df[absent_medpass_date_not_missed].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "MEDPASS_DATE is NULL and MEDPASS_RESULT is not 'Missed'"
    validation_failed.append(temp)
absent_result_date_not_missed = mpl_df["result_date"].isna() & (mpl_df["medpass_result"].astype(str).str.lower() != "missed")
if absent_result_date_not_missed.any():
    temp = mpl_df[absent_result_date_not_missed].copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "RESULT_DATE is NULL and MEDPASS_RESULT is not 'Missed'"
    validation_failed.append(temp)
dupe_df = mpl_df[mpl_df.duplicated(subset=["sapphire_pat_id", "facility_id", "formatted_ndc", "medpass_date", "result_date"], keep=False) & (mpl_df["medpass_result"].str.lower() != "missed")] # send mail if this exists 
if not dupe_df.empty:
    temp = dupe_df.copy()
    temp.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    temp["reason"] = "duplicate data for same SAPPHIRE_PAT_ID, FACILITY_ID, FORMATTED_NDC, MEDPASS_DATE, RESULT_DATE"
    validation_failed.append(temp)

if validation_failed:
    validation_failed_df = pd.concat(validation_failed, ignore_index=True)
    validation_failed_df.rename(columns={
        "sapphire_pat_id": "SAPPHIRE_PAT_ID",
        "facility_id": "FACILITY_ID",
        "order_id": "ORDER_ID",
        "prn_flag": "PRN_FLAG",
        "kop_flag": "KOP_FLAG",
        "label_name": "LABEL_NAME",
        "alt_name": "ALT_NAME",
        "strength": "STRENGTH",
        "dos_id": "DOS_ID",
        "formatted_ndc": "FORMATTED_NDC",
        "medpass_date": "MEDPASS_DATE",
        "result_date": "RESULT_DATE",
        "medpass_result": "MEDPASS_RESULT",
        "qty_dose": "QTY_DOSE",
        "recorded_by_full_name": "RECORDED_BY_FULL_NAME",
    })
else:
    validation_failed_df = pd.DataFrame(columns=list(mpl_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
# Find rows in mpl_df that are NOT in validation_df
clean_df = mpl_df.merge(
    validation_failed_df.drop(columns=["file_name", "reason"]),  # match only on original columns
    how="left",
    indicator=True
)
mpl_df = clean_df[clean_df["_merge"] == "left_only"].drop(columns=["_merge"])
mpl_df[["recorded_by_last_name", "recorded_by_first_name"]] = mpl_df["recorded_by_full_name"].str.split(",", n=1, expand=True)
mpl_df["recorded_by_first_name"] = mpl_df["recorded_by_first_name"].str.strip()
mpl_df["recorded_by_last_name"] = mpl_df["recorded_by_last_name"].str.strip()
mpl_df.drop(columns=["recorded_by_full_name"], inplace=True)
mpl_df = mpl_df[["sapphire_pat_id", "facility_id", "order_id", "prn_flag", "kop_flag", "formatted_ndc", "medpass_date", "result_date", "medpass_result", "qty_dose", "recorded_by_first_name", "recorded_by_last_name"]] # type: ignore
mpl_df.rename(columns={"sapphire_pat_id": "mrn", "order_id": "med_order_id", "prn_flag": "is_prn", "kop_flag": "is_kop", "formatted_ndc": "ndc", "medpass_date": "med pass_date", "result_date": "med_admin_date", "medpass_result": "med_admin_status", "qty_dose": "dose"}, inplace=True)
# %%
mpl_df
# %%
rx_relation = dataset["active_orders_with_rx_norm"]
rx_df = rx_relation.df()
rx_df = rx_df[~rx_df["facility_id"].str.match(r"^-+$", na=False)] # type: ignore
rx_df["file_date"] = rx_df["file_name"].str.extract(r"_(\d{8})_") # type: ignore
rx_df["file_date"] = pd.to_datetime(rx_df["file_date"], format="%Y%m%d") # type: ignore
max_date = rx_df["file_date"].max() # type: ignore
rx_df = rx_df[rx_df["file_date"] == max_date] # type: ignore
rx_df["ndc_code_of_drug_ordered"] = rx_df["ndc_code_of_drug_ordered"].astype(str).str.replace("-", "", regex=False)
# Vectorized validations
validation_conditions = [
    (rx_df["facility_id"].isna() | (rx_df["facility_id"].astype(str).str.strip() == ""), "FACILITY_ID is None or Nan or empty"),
    (rx_df["date_time_order_received"].notna() & pd.to_datetime(rx_df["date_time_order_received"], errors="coerce").isna(), "DATE_TIME_ORDER_RECEIVED is not a datetime"),
    (rx_df["ndc_code_of_drug_ordered"].isna() | (rx_df["ndc_code_of_drug_ordered"].astype(str).str.strip() == "") | pd.to_numeric(rx_df["ndc_code_of_drug_ordered"], errors="coerce").isna(), "NDC_CODE_OF_DRUG_ORDERED is NULL or empty or non numeric"),
    (rx_df["effective_date"].notna() & pd.to_datetime(rx_df["effective_date"], errors="coerce").isna(), "EFFECTIVE_DATE is not a date"),
    (rx_df["duration"].notna() & ~rx_df["duration"].astype(str).str.isdigit(), "DURATION is not an integer"),
    (rx_df["number_of_refills_issued"].notna() & ~rx_df["number_of_refills_issued"].astype(str).str.isdigit(), "NUMBER_OF_REFILLS_ISSUED is not an integer"),
    (rx_df["expiration_date"].notna() & pd.to_datetime(rx_df["expiration_date"], errors="coerce").isna(), "EXPIRATION_DATE is not a date"),
    (rx_df["dispensed_quantity"].notna() & ~rx_df["dispensed_quantity"].astype(str).str.split(".").str[0].str.isdigit(), "DISPENSED_QUANTITY is not an integer"),
    (rx_df["formulary_or_non_formulary_drug_flag"].notna() & ~rx_df["formulary_or_non_formulary_drug_flag"].astype(str).str.lower().isin(["yes", "no"]), "FORMULARY_OR_NON_FORMULARY_DRUG_FLAG is not 'Yes' or 'No'"),
    (rx_df["keep_on_person_indicator"].notna() & ~rx_df["keep_on_person_indicator"].astype(str).str.lower().isin(["pill line", "kop"]), "KEEP_ON_PERSON_INDICATOR is not 'Pill Line' or 'KOP'"),
    (rx_df["medication_time_of_day"].notna() & ~rx_df["medication_time_of_day"].astype(str).str.split(",").apply(lambda times: all(pd.notna(pd.to_datetime(t.strip(), format="%H:%M", errors="coerce")) for t in times if t.strip())), "MEDICATION_TIME_OF_DAY is not in valid 24-hour format (HH:MM)"),
    (rx_df["order_id"].isna() | (rx_df["order_id"].astype(str).str.strip() == ""), "ORDER_ID is NULL")
]

rx_df["validation_errors"] = ""
for condition, message in validation_conditions:
    mask = condition & (rx_df["validation_errors"] != "")
    rx_df.loc[condition & ~mask, "validation_errors"] = message
    rx_df.loc[mask, "validation_errors"] += "; " + message

rx_df["validation_errors"] = rx_df["validation_errors"].replace("", None)
failed_rows = rx_df[rx_df["validation_errors"].notna()].copy()

# Check for duplicates
dupe_df = rx_df[rx_df.duplicated(subset=["facility_id", "ndc_code_of_drug_ordered", "order_id"], keep=False)]
if not dupe_df.empty:
    dupe_df = dupe_df.copy()
    dupe_error = "duplicate data for same FACILITY_ID, NDC_CODE_OF_DRUG_ORDERED"
    dupe_df["validation_errors"] = dupe_df["validation_errors"].fillna("") + ("; " + dupe_error if dupe_df["validation_errors"].notna() else dupe_error)
    failed_rows = pd.concat([failed_rows, dupe_df[~dupe_df.index.isin(failed_rows.index)]], ignore_index=True)
if not failed_rows.empty:
    validation_failed_df = failed_rows.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"])
    validation_failed_df.rename(columns={
        "facility_id": "FACILITY_ID",
        "unit": "UNIT",
        "npi_number_of_ordering_practitioner": "NPI Number of ordering Practitioner",
        "name_of_ordering_provider": "Name of Ordering Provider",
        "date_time_order_received": "Date & Time order Received",
        "ndc_code_of_drug_ordered": "NDC Code of Drug ordered",
        "drug_name": "Drug Name",
        "effective_date": "Effective Date",
        "dosage": "Dosage",
        "drug_strength": "Drug Strength",
        "frequency": "Frequency",
        "duration": "Duration",
        "route": "Route",
        "number_of_refills_issued": "Number of Refills Issued",
        "expiration_date": "Expiration Date",
        "provider_admin_instructions": "Provider Admin Instructions",
        "dispensed_quantity": "Dispensed Quantity",
        "formulary_or_non_formulary_drug_flag": "Formulary Or Non Formulary Drug Flag",
        "keep_on_person_indicator": "Keep On Person Indicator",
        "medication_time_of_day": "What time of day the medication should be taken at (AM, PM, NOON, BEDTIME)",
        "order_id": "ORDER_ID",
        "ordered_by": "ORDERED_BY",
        "validation_errors": "reason"
    })
else:
    validation_failed_df = pd.DataFrame(columns=list(rx_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"], inplace=True)
rx_df = rx_df[~rx_df["ndc_code_of_drug_ordered"].isin(validation_failed_df["ndc_code_of_drug_ordered"])]
rx_df["medication_time_of_day"] = rx_df["medication_time_of_day"].str.split(",")
rx_df = rx_df[["facility_id", "unit", "npi_number_of_ordering_practitioner", "name_of_ordering_provider", "date_time_order_received", "ndc_code_of_drug_ordered",  "effective_date", "frequency", "duration", "route", "number_of_refills_issued", "expiration_date", "provider_admin_instructions", "dispensed_quantity", "formulary_or_non_formulary_drug_flag", "keep_on_person_indicator", "medication_time_of_day", "order_id", "ordered_by"]]
rx_df[["ordered_by_last_name", "ordered_by_first_name"]] = rx_df["name_of_ordering_provider"].str.split(",", n=1, expand=True)
rx_df[["entered_by_last_name", "entered_by_first_name"]] = rx_df["ordered_by"].str.split(",", n=1, expand=True)
rx_df["ordered_by_first_name"] = rx_df["ordered_by_first_name"].str.strip()
rx_df["ordered_by_last_name"] = rx_df["ordered_by_last_name"].str.strip()
rx_df["entered_by_first_name"] = rx_df["entered_by_first_name"].str.strip()
rx_df["entered_by_last_name"] = rx_df["entered_by_last_name"].str.strip()
rx_df.drop(columns=["name_of_ordering_provider", "ordered_by"], inplace=True)
rx_df.rename(columns={
    "unit": "loc_unit",
    "npi_number_of_ordering_practitioner": "npi",
    "date_time_order_received": "order_date",
    "ndc_code_of_drug_ordered": "ndc",
    "effective_date": "effective_date",
    "frequency": "frequency",
    "duration": "duration",
    "route": "route",
    "number_of_refills_issued": "refill_count",
    "expiration_date": "order_expiration_date",
    "provider_admin_instructions": "admin_instructions",
    "dispensed_quantity": "order_quantity",
    "formulary_or_non_formulary_drug_flag": "is_formulary",
    "keep_on_person_indicator": "is_kop",
    "medication_time_of_day": "admin_time",
    "order_id": "order_id"
}, inplace=True)
# %%
rx_df
# %%
ap_relation = dataset["active_probs"]
ap_df = ap_relation.df()
ap_df = ap_df[~ap_df["sapphire_pat_id"].str.match(r"^-+$", na=False)] # type: ignore
ap_df["file_date"] = ap_df["file_name"].str.extract(r"_(\d{8})_") # type: ignore
ap_df["file_date"] = pd.to_datetime(ap_df["file_date"], format="%Y%m%d") # type: ignore
max_date = ap_df["file_date"].max() # type: ignore
ap_df = ap_df[ap_df["file_date"] == max_date] # type: ignore
validation_conditions = [
    (ap_df["facility_id"].isna() | (ap_df["facility_id"].astype(str).str.strip() == ""), "FACILITY_ID is None or Nan or empty"),
    (ap_df["sapphire_pat_id"].isna() | (ap_df["sapphire_pat_id"].astype(str).str.strip() == ""), "SAPPHIRE_PAT_ID is None or Nan or empty"),
    (ap_df["date_created"].notna() & pd.to_datetime(ap_df["date_created"], errors="coerce").isna(), "DATE_CREATED is not a datetime")
]
ap_df["validation_errors"] = ""
for condition, message in validation_conditions:
    mask = condition & (ap_df["validation_errors"] != "")
    ap_df.loc[condition & ~mask, "validation_errors"] = message
    ap_df.loc[mask, "validation_errors"] += "; " + message
ap_df["validation_errors"] = ap_df["validation_errors"].replace("", None)
failed_rows = ap_df[ap_df["validation_errors"].notna()].copy()
if not failed_rows.empty:
    validation_failed_df = failed_rows.drop(columns=["_dlt_load_id", "_dlt_id", "file_date"])
    validation_failed_df.rename(columns={
        "sapphire_pat_id": "SAPPHIRE_PAT_ID",
        "facility_id": "FACILITY_ID",
        "code_set": "CODE_SET",
        "code": "CODE",
        "long_description": "LONG_DESCRIPTION",
        "date_created": "DATE_CREATED",
        "validation_errors": "reason"
    })
else:
    validation_failed_df = pd.DataFrame(columns=list(ap_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id", "file_date", "file_name", "validation_errors", "reason"], inplace=True)
ap_df.drop(columns=["file_name", "_dlt_load_id", "_dlt_id", "file_date", "validation_errors"], inplace=True)
clean_df = ap_df.merge(
    validation_failed_df.drop(columns=[col for col in ["file_name", "validation_errors", "reason"] if col in validation_failed_df.columns]),  # match only on original columns
    how="left",
    indicator=True
)
ap_df = clean_df[clean_df["_merge"] == "left_only"].drop(columns=["_merge"])
ap_df.rename(columns={
    "facility_id": "facility_id",
    "sapphire_pat_id": "mrn",
    "code_set": "code_set",
    "code": "icd_code",
    "long_description": "icd_description",
    "date_created": "entered_date"
}, inplace=True)
# %%
avo_relation = dataset["active_vital_tx_orders"]
avo_df = avo_relation.df()
avo_df = avo_df[~avo_df["sapphire_pat_id"].str.match(r"^-+$", na=False)] # type: ignore
validation_conditions = [
    (avo_df["facility_id"].isna() | (avo_df["facility_id"].astype(str).str.strip() == ""), "FACILITY_ID is None or Nan or empty"),
    (avo_df["sapphire_pat_id"].isna() | (avo_df["sapphire_pat_id"].astype(str).str.strip() == ""), "SAPPHIRE_PAT_ID is None or Nan or empty"),
    (avo_df["time_slots"].notna() & ~avo_df["time_slots"].astype(str).str.split(",").apply(lambda times: all(pd.notna(pd.to_datetime(t.strip(), format="%H:%M", errors="coerce")) for t in times if t.strip())), "TIME_SLOTS is not in valid 24-hour format (HH:MM)"),
    (avo_df["start_date"].notna() & ~pd.to_datetime(avo_df["start_date"], errors="coerce").notna(), "START_DATE is not a datetime"),
    (avo_df["stop_date"].notna() & ~pd.to_datetime(avo_df["stop_date"], errors="coerce").notna(), "STOP_DATE is not a datetime"),
    (avo_df["vital_order_id"].isna() | (avo_df["vital_order_id"].astype(str).str.strip() == ""), "VITAL_ORDER_ID is None or Nan or empty"),]
avo_df["validation_errors"] = ""
for condition, message in validation_conditions:
    mask = condition & (avo_df["validation_errors"] != "")
    avo_df.loc[condition & ~mask, "validation_errors"] = message
    avo_df.loc[mask, "validation_errors"] += "; " + message
avo_df["validation_errors"] = avo_df["validation_errors"].replace("", None)
failed_rows = avo_df[avo_df["validation_errors"].notna()].copy()
dupe_df = avo_df[avo_df.duplicated(subset=["vital_order_id"], keep=False)]
if not dupe_df.empty:
    dupe_df = dupe_df.copy()
    dupe_error = "duplicate data for same VITAL_ORDER_ID"
    mask = dupe_df["validation_errors"].notna()
    dupe_df.loc[mask, "validation_errors"] = dupe_df.loc[mask, "validation_errors"] + "; " + dupe_error
    dupe_df.loc[~mask, "validation_errors"] = dupe_error
    failed_rows = pd.concat([failed_rows, dupe_df[~dupe_df.index.isin(failed_rows.index)]], ignore_index=True)
if not failed_rows.empty:
    validation_failed_df = failed_rows.drop(columns=["_dlt_load_id", "_dlt_id"])
    validation_failed_df.rename(columns={
        "facility_id": "FACILITY_ID",
        "sapphire_pat_id": "SAPPHIRE_PAT_ID",
        "vital_order_type": "VITAL_ORDER_TYPE",
        "directions": "DIRECTIONS",
        "frequency": "FREQUENCY",
        "prescriber": "PRESCRIBER",
        "npi": "NPI",
        "time_slots": "TIME_SLOTS",
        "start_date": "START_DATE",
        "stop_date": "STOP_DATE",
        "vital_order_id": "VITAL_ORDER_ID",
        "ordered_by": "ORDERED_BY",
        "validation_errors": "reason"
    }, inplace=True)
else:
    validation_failed_df = pd.DataFrame(columns=list(avo_df.columns) + ["reason"])
    validation_failed_df.drop(columns=["_dlt_load_id", "_dlt_id"], inplace=True)
    validation_failed_df.rename(columns={
        "vital_order_id": "VITAL_ORDER_ID"
    }, inplace=True)
avo_df = avo_df[~avo_df["vital_order_id"].isin(validation_failed_df["VITAL_ORDER_ID"])]
avo_df["time_slots"] = avo_df["time_slots"].fillna("").str.split(",")
avo_df["time_slots"] = avo_df["time_slots"].apply(lambda x: [] if x == [''] else x)
avo_df
# %%
avo_df[["ordered_by_last_name", "ordered_by_first_name"]] = avo_df["prescriber"].str.split(",", n=1, expand=True)
avo_df[["entered_by_last_name", "entered_by_first_name"]] = avo_df["ordered_by"].str.split(",", n=1, expand=True)
avo_df["ordered_by_first_name"] = avo_df["ordered_by_first_name"].str.strip()
avo_df["ordered_by_last_name"] = avo_df["ordered_by_last_name"].str.strip()
avo_df["entered_by_first_name"] = avo_df["entered_by_first_name"].str.strip()
avo_df["entered_by_last_name"] = avo_df["entered_by_last_name"].str.strip()
avo_df.drop(columns=["prescriber", "ordered_by"], inplace=True)
avo_df.drop(columns=["file_name", "_dlt_load_id", "_dlt_id", "validation_errors"], inplace=True)
avo_df.rename(columns={
    "facility_id": "facility_id",
    "sapphire_pat_id": "mrn",
    "vital_order_type": "vital_order",
    "directions": "vital_instructions",
    "frequency": "frequency",
    "npi": "npi",
    "time_slots": "vital_time",
    "start_date": "vital_start_date",
    "stop_date": "vital_stop_date",
    "vital_order_id": "vital_order_id"
}, inplace=True)
# %%
avo_df.to_csv("avo_df.csv", index=False)
# %%
