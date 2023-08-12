import os, time
import pyarrow.parquet as pq
from time import sleep
from metaflow import Flow, FlowSpec, step, retry, batch #, trigger_on_finish
from elastic import get_elastic_client
from daily_values import daily_values, superfoods, calculate_nutrients_score, check_if_id_is_in_es_index
from utils import download_from_s3, list_files_in_folder, upload_to_s3, read_data
from config import bucket_name, index, image


#@trigger_on_finish(flow='SfUsdaFlow')
class SfUsdaToEsFlow(FlowSpec):
    """
    A Metaflow flow for ingesting docs into elastic index    
    """
    @batch(cpu=1, memory=3500, image=image)
    @step
    def start(self):
        """Fetch a list of json files with data from usda db."""

        self.bucket_name = bucket_name
        self.prefix = f"{index}/usda"
        self.docs = [] # for pylint
        self.fields = [] # for pylint

        contents = list_files_in_folder(self.bucket_name, self.prefix)
        self.files = contents.get(self.prefix.split("/")[-1])
        self.next(self.download, foreach='files')


    @batch(cpu=1, memory=3500, image=image)
    #@retry(times=3, minutes_between_retries=1)
    @step
    def download(self):
        """Download the files in parallel from S3. Save content as artifacts in metaflow"""

        print(self.input)
        self.filename = self.input
        self.bucket_name = bucket_name
        self.prefix = f"{index}/usda"
        
        # check if the file already exists on S3
        if self.filename:
            self.s3_path = os.path.join(f's3://{self.bucket_name}', self.prefix, f'{self.filename}')

            # foreach is run on a new instance every time
            os.makedirs(os.path.join(self.bucket_name, self.prefix), exist_ok=True)

            file_path = download_from_s3(self.s3_path)
            self.docs = read_data([file_path], "nutrients")             

        self.next(self.join)


    @batch(cpu=2, memory=7500)
    @step
    def join(self, inputs):
        """Join intermediate results into a single self.docs artifact"""
        if not hasattr(self, 'docs'):
            self.docs = []

        for input in inputs:
            self.docs.extend(input.docs)  # Extend self.docs with the docs from each input

        self.num_docs = len(self.docs)
        self.next(self.load_archived)

    @batch(cpu=2, memory=7500, image=image)
    @retry(times=2, minutes_between_retries=1)
    @step
    def load_archived(self):
        """fetch all archived (processed) nutrients docs from the mlops-nutrients bucket"""

        # Specify the S3 bucket and file path
        self.bucket_name = bucket_name
        # nutrients.parquet contains docs in labeled index (training, valiadtion, testing)
        #self.file = f'{index}.parquet'
        # fullset.parquet contains all docs including not labeled
        self.file = 'fullset.parquet'
        self.prefix = index
        
        # Read the Parquet file from S3 into a PyArrow Table
        table = pq.read_table(f's3://{self.bucket_name}/{self.prefix}/{self.file}')

        if table:
            # Convert the PyArrow Table to a pandas DataFrame
            df = table.to_pandas()

        if not df.empty:
            # Convert the DataFrame back into docs (if necessary)
            self.archived_docs = df.to_dict(orient='records')

        self.next(self.upload_archived)


    @batch(cpu=2, memory=7500, image=image)
    @retry(times=2, minutes_between_retries=1)
    @step
    def upload_archived(self):
        """ write archived docs into elastic search index """
        
        if self.archived_docs:
            # Specify the Elasticsearch index name
            self.to_index = index
            # Connect to Elasticsearch
            es = get_elastic_client("local")

            # Iterate over each doc and index it in Elasticsearch
            for doc in self.archived_docs:
                # Index the document
                response = es.index(index=self.to_index, document=doc)
                
                # Optionally, you can check the response for success
                if response["result"] != "created":
                    print(f"Failed to index document: {doc}")

            # Refresh the index to make the changes visible for search operations
            es.indices.refresh(index=self.to_index)

        self.num_docs = len(self.archived_docs)
        #self.next(self.upload_to_index)
        self.next(self.end)

    # @batch(cpu=2, memory=7500, image=image)
    # #@retry(times=3, minutes_between_retries=1)
    # @step
    # def upload_to_index(self):
    #     """ fetch additional data which has not been processed yet - (this code hits a usda api with 1000 calls an hour limit, hence is very slow ) """

    #     self.to_index = index

    #     # client = get_elastic_client("cloud")
    #     client = get_elastic_client("local")

    #     for doc in self.docs:
    #         fdc_id = doc.get("fdc_id")

    #         # only add new ones
    #         if not check_if_id_is_in_es_index(self.to_index,fdc_id):
    #             score, labelData = calculate_nutrients_score(doc)
    #             doc['score'] = score
    #             doc['labelNutrients'] = labelData
                
    #             if doc.get("food_nutrients"):
    #                 del doc["food_nutrients"]

    #             # Check if nutrients field is an object
    #             if not isinstance(doc.get("nutrients"), dict):
    #                 doc["nutrients"] = {}  # Set nutrients field as an empty object
                
    #             # alternatively fetch addiitonal data from another archived index

    #             json_doc = json.dumps(doc) 
    #             client.index(index=self.to_index, document=json_doc)    

    #     self.num_docs = len(self.docs)
    #     self.next(self.end)

    @batch(cpu=1, memory=3500)
    @retry(times=2, minutes_between_retries=1)
    @step
    def end(self):
        """End the flow"""
        print(f"Uploaded to index: {self.to_index} - {self.num_docs} docs")


if __name__ == '__main__':
    flow = SfUsdaToEsFlow()
    flow.run()

