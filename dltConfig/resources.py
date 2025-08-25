import dlt
from dlt.sources.filesystem import filesystem, read_csv
from datetime import datetime, timezone
from models.raw import ErrorLogs

class DLTResource:
    def __init__(self, table_name, columns, schema_contract, write_disposition, bucket_url, file_glob):
        self.table_name = table_name
        self.columns = columns
        self.schema_contract = schema_contract
        self.write_disposition = write_disposition
        self.bucket_url = bucket_url
        self.file_glob = file_glob
    
    def create_filesystem_resource(self):
        @dlt.resource(
            table_name=self.table_name, 
            columns=self.columns, 
            schema_contract=self.schema_contract, 
            write_disposition=self.write_disposition)
        def filesystem_resource():
            files = filesystem(bucket_url=self.bucket_url, file_glob=self.file_glob)
            files.apply_hints(incremental=dlt.sources.incremental("modification_date", initial_value=datetime(2025, 8, 10, tzinfo=timezone.utc)))
            files_pipe = files | read_csv()
            return files_pipe.with_name(self.table_name)
        return filesystem_resource
    
    def create_error_resource(self, pipeline_name=None, err=None):
        @dlt.resource(
            table_name="error_logs", 
            columns=ErrorLogs, 
            schema_contract="evolve",
            write_disposition="append"
        )
        def error_resource():
            if err:
                yield {
                    "pipeline_name": pipeline_name,
                    "datetime": datetime.now(timezone.utc),
                    "error_message": str(err)
                }
            else:
                yield {}
        
        return error_resource
