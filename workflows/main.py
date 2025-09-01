import dlt
from models.file_table_mapping import file_table_mapping
from dlt.sources.filesystem import read_csv
from dltConfig.resources import DLTResource
from dltConfig.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed
from dotenv import load_dotenv
import os
import shutil
import glob
load_dotenv()

if __name__ == "__main__":

    clickhouse_destination = dlt.destinations.clickhouse()
    models = file_table_mapping

    error_logger = DLTResource(
        table_name="error_logs",
        schema_contract="evolve",
        columns=None,
        write_disposition="append",
        file_glob=None 
    )
    error_logging_pipeline = Pipeline(
        source=[error_logger.create_error_resource(None, None)],
        destination=clickhouse_destination,
        pipeline_name="error_logging",
        pipelines_dir=os.getenv("PIPELINES_DIR")
    )
    
    for table_name, model, file_glob, column_mapper in models:
            source_data = DLTResource(
                table_name=table_name,
                columns=model,
                schema_contract={
                    "data_type": "evolve",
                    "tables": "evolve",
                    "columns": "freeze"
                },
                write_disposition="append",
                file_glob=file_glob
            )

            for file in source_data.list_files():
                print(file)
                file_resource = source_data.create_filesystem_resource(file, column_mapper=column_mapper)
                pipeline_name = f"fch_analytics_{table_name.lower()}_{file.split('.')[0]}"
                pipeline = Pipeline(
                    source= file_resource,
                    destination=clickhouse_destination,
                    pipeline_name=pipeline_name,
                    pipelines_dir=os.getenv("PIPELINES_DIR")
                )
                try:
                    extracted_folders = glob.glob(f"{os.getenv('PIPELINES_DIR')}/fch_analytics_*/normalize/extracted")
                    for folder in extracted_folders:
                        if os.path.exists(folder):
                            shutil.rmtree(folder)
                    print(f"Pipeline: {pipeline_name}, File: {file}")
                    load_info = pipeline.run()
                    
                    source_data.move_file_to_processed(file, "fch-analytics-testing/incoming", "fch-analytics-testing/processed")
                    
                except PipelineStepFailed as step_failed:
                    error_logging_pipeline.set_source(error_logger.create_error_resource(pipeline_name, f'''PipelineStep: {step_failed.step}, File: {file}, Pipeline: {pipeline_name}, Error: {str(step_failed)}'''))
                    error_logging_pipeline.run()
                    source_data.move_file_to_processed(file, "fch-analytics-testing/incoming", "fch-analytics-testing/failed")
                    continue
    pipelines_dir = os.getenv('PIPELINES_DIR', '')
    if os.path.exists(pipelines_dir):
        shutil.rmtree(pipelines_dir)
