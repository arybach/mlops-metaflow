import json
from time import sleep
from metaflow import Flow, FlowSpec, step, retry, batch
from elastic import get_elastic_client
from daily_values import daily_values, superfoods, calculate_nutrients_score, check_if_id_is_in_es_index

class SfUsdaToEsFlow(FlowSpec):
    """
    A Metaflow flow for extracting frames from a video file in parallel using Dask.    
    """
    #@batch(cpu=1, memory=3500, image=image)
    @step
    def start(self):
        """Start the flow and get docs from a prior flow."""

        # Get streams from a prior run
        download_flow_res = Flow("SfUsdaFlow")
        self.docs = download_flow_res.latest_run["end"].task.data.docs

        self.next(self.upload_to_index)


    #@batch(cpu=1, memory=3500, image=image)
    #@retry(times=3, minutes_between_retries=1)
    @step
    def upload_to_index(self):

        self.index_name = "nutrients"

        # client = get_elastic_client("cloud")
        client = get_elastic_client("local")

        for doc in self.docs:
            fdc_id = doc.get("fdc_id")

            # only add new ones 
            if not check_if_id_is_in_es_index(self.index_name,fdc_id):

                score, labelData = calculate_nutrients_score(doc)
                doc['score'] = score
                doc['labelNutrients'] = labelData
                
                if doc.get("food_nutrients"):
                    del doc["food_nutrients"]

                # Check if nutrients field is an object
                if not isinstance(doc.get("nutrients"), dict):
                    doc["nutrients"] = {}  # Set nutrients field as an empty object

                json_doc = json.dumps(doc) 
                client.index(index=self.index_name, document=json_doc)    
    
        self.next(self.end)

    #@batch(cpu=1, memory=3500, image=image)
    @step
    def end(self):
        """End the flow"""
        print(f"Uploaded to index: ${self.index_name} - ${len(self.docs)} docs")

if __name__ == '__main__':
    flow = SfUsdaToEsFlow()
    flow.run()

