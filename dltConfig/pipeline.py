import dlt

class Pipeline:
    def __init__(self, source, destination, dataset_name, pipelines_dir=None, pipeline_name="default", import_schema_path=None):
        self.pipeline_name = pipeline_name
        self.dataset_name = dataset_name
        self.destination = destination
        self.source = source
        self.import_schema_path = import_schema_path
        self.pipelines_dir = pipelines_dir
        self.pipeline =  dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            pipelines_dir=self.pipelines_dir,
            dataset_name=self.dataset_name
        )
        
    def run(self):
        load_info = self.pipeline.run(self.source)
        return load_info
    
    def set_source(self, source):
        self.source = source

    def set_pipeline_name(self, pipeline_name):
        self.pipeline_name = pipeline_name