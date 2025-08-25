import dlt

class Pipeline:
    def __init__(self, source, destination, pipeline_name="default"):
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.source = source
        
    def run(self):
        load_info = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination

        )
        load_info = load_info.run(self.source)
        return load_info
    
    def set_source(self, source):
        self.source = source