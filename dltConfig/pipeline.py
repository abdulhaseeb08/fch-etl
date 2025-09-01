import dlt

class Pipeline:
    def __init__(self, source, destination, pipelines_dir=None, pipeline_name="default", import_schema_path=None):
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.source = source
        self.import_schema_path = import_schema_path
        self.pipelines_dir = pipelines_dir
        
    def run(self):
        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            #import_schema_path=self.import_schema_path,
            pipelines_dir=self.pipelines_dir
        )
        load_info = pipeline.run(self.source)
        return load_info
    
    def set_source(self, source):
        self.source = source