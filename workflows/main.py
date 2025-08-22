import dlt
from dlt.sources.filesystem import filesystem, read_csv_duckdb
from models.raw import *
from dltConfig.resources import DLTResource
from dltConfig.piepline import Pipeline

# ClickHouse destination configuration
clickhouse_destination = dlt.destinations.clickhouse(
    credentials={
        "database": "rawData",
        "username": "default", 
        "password": "august",
        "host": "localhost",
        "http_port": 8123,
        "port": 9000,
        "secure": 0
    }
)

# Create resources for each Pydantic model
models = [
    #("MedPassResults", MedPassResults, "*MedPassResults*.csv"),
    #("VitalResults", VitalResults, "*VitalResults*.csv"),
    #("TasksApptsActivity", TasksApptsActivity, "*TasksApptsActivity*.csv"),
    #("PhysicalAssessmentWithClass", PhysicalAssessmentWithClass, "*PhysicalAssessment*.csv"),
    #("RosterReleases", RosterReleases, "*RosterReleases*.csv"),
    #("TBTestResults", TBTestResults, "*TBTestResults*.csv"),
    #("FormResponseCaptures", FormResponseCaptures, "*FormResponseCaptures*.csv"),
    #("FormSubmissions", FormSubmissions, "*FormSubmissions*.csv"),
    #("LabResults", LabResults, "*LabResults*.csv"),
    ("ActiveVitalTxOrders", ActiveVitalTxOrders, "*ActiveVitalandTXOrders*.csv"),
    #("ActiveRoster", ActiveRoster, "*ActiveRoster*.csv"),
    #("ActiveProbs", ActiveProbs, "*ActiveProbs*.csv"),
    #("ActiveOrdersWithRxNorm", ActiveOrdersWithRxNorm, "*ActiveORDERSWithRXNorm*.csv"),
    #("Files", Files, "*Files*.csv"),
]

bucket_url = "file://home/haseeb/Desktop/gcp_to_clickhouse/demo_pipeline/bucket/incoming"

# Create resources
resources = []
for table_name, model_class, file_glob in models:
    resource = DLTResource(
        table_name=table_name,
        columns=model_class,
        schema_contract="evolve",
        write_disposition="append",
        bucket_url=bucket_url,
        file_glob=file_glob
    )
    resources.append(resource.create_filesystem_resource())
# Run pipeline
pipeline = Pipeline(
    source=resources,
    destination=clickhouse_destination,
    pipeline_name="healthcare_data_pipeline"
)

if __name__ == "__main__":
    load_info = pipeline.run()
    print(f"Pipeline completed successfully: {load_info}")
