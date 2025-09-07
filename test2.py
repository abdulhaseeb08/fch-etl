# %%
import dlt
from dlt.sources.filesystem import filesystem, read_csv
from models.raw import *

@dlt.transformer()
def safe_read_csv(items):
    for item in items:
        try:
            a =  (filesystem(bucket_url="file://home/haseeb/Desktop/gcp_to_clickhouse/demo_pipeline/bucket/incoming", file_glob=item['file_name']) | read_csv())
            def add_file_name_column(row):
                row["file_name"] = item['file_name']
                return row
            final_file_pipe = a | add_file_name_column
            yield from final_file_pipe
            print(f"Processed file {item['file_name']}")
        except Exception as e:
            print(f"Failed to process file {item['file_name']}: {e}")

a = filesystem(bucket_url="file://home/haseeb/Desktop/gcp_to_clickhouse/demo_pipeline/bucket/incoming", file_glob="*TBTestResults*.csv")
a = a | safe_read_csv()

# %%
for item in a:
    print(item)
# %%
