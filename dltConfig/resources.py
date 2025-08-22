import dlt
from dlt.sources.filesystem import filesystem, read_csv_duckdb
from models.raw import *

class DLTResource:
    def __init__(self, table_name, columns, schema_contract, write_disposition, bucket_url, file_glob):
        self.table_name = table_name
        self.columns = columns
        self.schema_contract = schema_contract
        self.write_disposition = write_disposition
        self.bucket_url = bucket_url
        self.file_glob = file_glob
    
    def create_filesystem_resource(self):
        @dlt.resource(table_name=self.table_name, columns=self.columns, schema_contract=self.schema_contract, write_disposition=self.write_disposition, )
        def filesystem_resource():
            return filesystem(bucket_url=self.bucket_url, file_glob=self.file_glob) | read_csv_duckdb()
        return filesystem_resource
    
    def create_file_logging_resource(self):
        @dlt.resource(table_name="files", columns=Files, write_disposition="append")
        def file_logging_resource():
            return filesystem(bucket_url=self.bucket_url, file_glob=self.file_glob) | read_csv_duckdb()
        return file_logging_resource