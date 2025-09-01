import dlt
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource
from datetime import datetime, timezone
import os
from utils.header_to_column_mapper import rename_columns

class DLTResource:
    def __init__(self, table_name,schema_contract, columns, write_disposition,file_glob):
        self.table_name = table_name
        self.columns = columns
        self.schema_contract = schema_contract
        self.write_disposition = write_disposition
        self.file_glob = file_glob

    def list_files(self):
        files = filesystem(file_glob=self.file_glob)
        files_to_process = []
        for file in files:
            files_to_process.append(file['file_name'])
        return files_to_process
    
    def create_filesystem_resource(self, file_to_process, column_mapper=None):
        @dlt.resource(
            table_name=self.table_name,
            columns=self.columns,
            schema_contract=self.schema_contract, 
            write_disposition=self.write_disposition)
        def filesystem_resource():
            file = filesystem(file_glob=file_to_process)
            file.apply_hints(incremental=dlt.sources.incremental("modification_date", initial_value=datetime(2025, 8, 25, tzinfo=timezone.utc)))
            file_pipe = file | read_csv()
            renamed_columns = file_pipe.add_map(rename_columns(column_mapper))
            file_name = os.path.basename(file_to_process)
            def add_file_name_column(row):
                row["file_name"] = file_name
                return row
            final_file_pipe = renamed_columns | add_file_name_column
            
            return final_file_pipe.with_name(self.table_name)
        return filesystem_resource
    
    def move_file_to_processed(self, file_to_move, path_one, path_two):
        fs_resource = filesystem(file_glob=file_to_move)
        fs_client = fsspec_from_resource(fs_resource)
        
        source_path = f"{path_one}/{file_to_move}"
        processed_path = f"{path_two}"
        
        fs_client.mv(source_path, processed_path)
    
    def create_error_resource(self, pipeline_name=None, err=None):
        @dlt.resource(
            table_name="error_logs",
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
