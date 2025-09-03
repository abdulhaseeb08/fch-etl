# %%
import dlt
from dlt.sources.filesystem import filesystem, read_csv_duckdb
from models.raw import *

@dlt.resource(table_name="ActiveVitalTxOrders", columns=ActiveVitalTxOrders, schema_contract="evolve", write_disposition="append")
def filesystem_resource():
    return filesystem(bucket_url="file://home/haseeb/Desktop/gcp_to_clickhouse/demo_pipeline/bucket/incoming", file_glob="incoming_20250820_ActiveVitalandTXOrders.csv") | read_csv_duckdb()

@dlt.resource(table_name="files", columns=ErrorLogs, write_disposition="append")
def file_logging_resource():
    return [{"file_name": "ActiveProbs.csv", "status": "success", "error_message": None, "load_time": "2025-08-22 10:00:00"}]
pipeline_one = dlt.pipeline(
    pipeline_name="test_run_one", 
    destination=dlt.destinations.clickhouse(
        credentials={
            "database": "firstInstance",
            "username": "default", 
            "password": "august",
            "host": "localhost",
            "http_port": 8123,
            "port": 9000,
            "secure": 0
        }
    )
)

pipeline = dlt.pipeline(
    pipeline_name="test_run", 
    destination=dlt.destinations.clickhouse(
        credentials={
            "database": "firstInstance",
            "username": "default", 
            "password": "august",
            "host": "localhost",
            "http_port": 8123,
            "port": 9000,
            "secure": 0
        }
    )
)

load_info = pipeline.run([filesystem_resource, file_logging_resource])
# %%
dlt.source()

# %%
import pandas as pd # type: ignore
df = pd.read_csv("bucket/incoming/incoming_20250820_ActiveVitalandTXOrders.csv")
df.head()
# %%
import pickle
with open("trace.pickle", "rb") as f:
    trace_data = pickle.load(f)

print(type(trace_data))
print(trace_data)
# %%
