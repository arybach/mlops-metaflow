import json
from time import sleep
from metaflow import Flow, FlowSpec, step, retry, batch
from elastic import get_elastic_client
from daily_values import calculate_recipes_score

class SfDavinciToEsFlow(FlowSpec):
    """
    A Metaflow flow for extracting frames from a video file in parallel using Dask.    
    """
    #@batch(cpu=1, memory=3500, image=image)
    @step
    def start(self):
        """Start the flow and get docs from a prior flow."""

        # Get streams from a prior run
        download_flow_res = Flow("SfDavinciFlow")
        self.docs = download_flow_res.latest_run["end"].task.data.docs

        self.next(self.upload_to_index)


    #@batch(cpu=1, memory=3500, image=image)
    #@retry(times=3, minutes_between_retries=1)
    @step
    def upload_to_index(self):

        self.index_name = "recipes"

        # client = get_elastic_client("cloud")
        client = get_elastic_client("local")

        for doc in self.docs:
            #score = calculate_recipes_score(doc)
            #doc['score'] = score
            if doc:
                json_doc = json.dumps(doc)
                print("Directions - ", type(doc.get("directions")))

            if json_doc:
                client.index(index=self.index_name, document=json_doc)    
    
        self.next(self.end)

    #@batch(cpu=1, memory=3500, image=image)
    @step
    def end(self):
        """End the flow"""
        print(f"Uploaded to index: {self.index_name} - {len(self.docs)} docs")

if __name__ == '__main__':
    flow = SfDavinciToEsFlow()
    flow.run()

