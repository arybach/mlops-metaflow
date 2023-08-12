import pandas as pd
from elastic import get_elastic_client
from embeddings import get_model, calculate_vector_embeddings, models, ingest_embeddings_to_elasticsearch
from metaflow import FlowSpec, step, card, current, batch #, conda_base
from utils import upload_to_s3
from config import bucket_name, index, model, image
import numpy as np

# Add more libraries and their versions as needed - this is uber-inconvinient as you have to specify channel, package name and version (and these it before any ENVs!)
# @conda_base(python='3.9.17', 
#             libraries={
#                 'numpy': '1.24.3',
#                 'pandas': '2.0.2',
#                 'nltk': '3.8.1',
#                 'conda-forge::huggingface_hub': '0.15.1',
#                 'elasticsearch': '8.8.0',
#                 'conda-forge::openai': '0.27.8',
#                 "conda-forge::sentence-transformers": "2.2.2"
#             })
class SfEmbeddingsFlow(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @batch(cpu=3, memory=7500,image=image)
    @step
    def start(self):
        """fetch training docs from elastic search index"""

        self.bucket_name = bucket_name
        # Connect to Elasticsearch
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration

        # Define the index name
        self.index_name = index

        # Define the query to retrieve all documents
        query = {
            "query": {
                "match_all": {}
            },
            "size": 10000  # Adjust the size based on your requirements
        }
                # Initial request
        response = es.search(index=self.index_name, body=query, scroll="2m") # pylint: disable=E1123
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        docs = hits

        # Subsequent requests
        while True:
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            if not hits:
                break
            docs.extend(hits)

        # store docs as an artifact
        batchdocs = pd.DataFrame([doc["_source"] for doc in docs]).to_dict(orient='records')
        # split to 10 batches
        batches = np.array_split(batchdocs, 10)
        self.batches = [batch.tolist() for batch in batches]
        self.next(self.calculate_embeddings, foreach='batches')

    @card
    @batch(cpu=2, memory=7500,image=image)
    @step
    def calculate_embeddings(self):

        docs = self.input
        # print(docs)
        descriptions = [doc["description"] for doc in docs]
        doc_ids = [doc["id"] for doc in docs]

        # Define the index name (parent self.index_name is not passed in the foreach, only the for self.input=batch in self.batches)
        self.index_name = index

        # Calculate vector embeddings using a specific model
        self.model_name = model  # Choose the desired model
        embeddings = calculate_vector_embeddings(descriptions, models[self.model_name])

        # Ingest the embedding vectors back into the Elasticsearch index
        ingest_embeddings_to_elasticsearch(embeddings, self.index_name, doc_ids)

        # Create a DataFrame with IDs, descriptions, and embeddings
        data = {
            "id": doc_ids,
            "description": descriptions,
            "embedding": embeddings
        }
        self.batch_docs = pd.DataFrame(data).to_dict(orient='records')        
        self.next(self.join)

    @batch(cpu=2, memory=7500,image=image)
    @step
    def join(self, inputs):
        """ join batch embeddings into a single dataframe """
        self.docs = []
        if hasattr(self, 'docs'):
            # skip non-downloadable files
            self.docs.extend([i.batch_docs for i in inputs])
        else:
            self.docs = [i.batch_docs for i in inputs]

        self.embeddings_df = pd.DataFrame(self.docs)
        self.next(self.end)

    @batch(cpu=2, memory=3500)
    @step
    def end(self):
        """ Embeddings added """
        # Define the index name (parent self.index_name is not passed in the foreach, only the for self.input=batch in self.batches)
        self.index_name = "labeled"
        print(f"Embeddings added to {len(self.embeddings_df)} docs in {self.index_name} index")

if __name__ == '__main__':
    # this can be run with the following command if @conda_base is defined for the class:
    # CONDA_CHANNELS=conda-forge python3 sf_embeddings_flow.py --environment=conda run
    # or with @batch and 5 instances limit (there're 10 batches at the moment):
    # python3 sf_embeddings_flow.py run --max-workers 5
    flow = SfEmbeddingsFlow()
    flow.run()
    #flow.run('--environment=conda')