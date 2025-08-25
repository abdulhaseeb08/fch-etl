import dlt
from models.raw import *
from models.file_table_mapping import file_table_mapping
from dltConfig.resources import DLTResource
from dltConfig.pipeline import Pipeline
from dotenv import load_dotenv
import os
load_dotenv()

if __name__ == "__main__":

    clickhouse_destination = dlt.destinations.clickhouse()
    models = file_table_mapping
    bucket_url = os.getenv("BUCKET_URL")

    error_logger = DLTResource(
        table_name="error_logs",
        columns=None, 
        schema_contract="evolve",
        write_disposition="append",
        bucket_url=None,  
        file_glob=None 
    )
    error_logging_pipeline = Pipeline(
        source=[error_logger.create_error_resource(None, None)],
        destination=clickhouse_destination,
        pipeline_name="error_logging"
    )
    
    for table_name, model_class, file_glob in models:
        try:            
            source_data = DLTResource(
                table_name=table_name,
                columns=model_class,
                schema_contract="evolve",
                write_disposition="append",
                bucket_url=bucket_url,
                file_glob=file_glob
            )
            
            pipeline = Pipeline(
                source=[source_data.create_filesystem_resource()],
                destination=clickhouse_destination,
                pipeline_name=f"fch_analytics_{table_name.lower()}"
            )
            
            load_info = pipeline.run()
            
        except Exception as e:
            pipeline_name = f"fch_analytics_{table_name.lower()}"
            error_logging_pipeline.set_source(error_logger.create_error_resource(pipeline_name, e))
            error_logging_pipeline.run()
            continue
    
