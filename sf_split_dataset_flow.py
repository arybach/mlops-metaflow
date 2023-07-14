import random
from metaflow import Flow, FlowSpec, step, retry, batch
from elastic import get_elastic_client
import pandas as pd
import pyarrow.parquet as pq

class SfSplitDataset(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @step
    def start(self):
        """fetch all nutrients docs from elastic search index"""

        # Specify the S3 bucket and file path
        self.bucket_name = 'mlops-nutrients'
        self.file = 'nutrients.parquet'
        self.prefix = 'nutrients'
        
        # Read the Parquet file from S3 into a PyArrow Table
        table = pq.read_table(f's3://{self.bucket_name}/{self.prefix}/{self.file}')

        # Convert the PyArrow Table to a pandas DataFrame
        df = table.to_pandas()

        # Convert the DataFrame back into docs (if necessary)
        self.docs = df.to_dict(orient='records')
        self.next(self.split_dataset)

    @step
    def split_dataset(self):
        """randomly label 1/3 training, 1/3 validation, 1/3 testing"""

        # scroll batch size, max = 10000
        size = len(self.docs)
        # Keep track of the number of documents with score equal to 0
        # these are mostly irrelevant  for classification as they don't contain any nutrients of interest
        zero_score_limit = 0.15 * size  
        # 15% of the selected docs (with the limit set to 20% none of the docs will be filtered out)
        # this should be sufficient to cause data drift detection if tested for it 

        # Define the new index
        labeled_docs = []

        zeros = 0
        for doc in self.docs:
            
            label = random.choice(["training", "validation", "testing"])

            if doc['score'] ==0 and zeros > zero_score_limit:
                continue
            else:
                zeros += 1
            # Update the document with the new label and index
            doc.update({"label": label})
            
            labeled_docs.append(doc)

        # Print the sizes of the datasets
        total_docs = len(labeled_docs)
        train_size = int(total_docs/3)
        val_size = int(total_docs/3)
        test_size = total_docs - train_size - val_size

        print("Training dataset size:", train_size)
        print("Validation dataset size:", val_size)
        print("Testing dataset size:", test_size)
        self.labeled_docs = labeled_docs
        self.next(self.ingest_to_index)

    @step
    def ingest_to_index(self):
        """ write labeled docs into elastic search labeled index """
        # Specify the Elasticsearch index name
        self.to_index = 'labeled'
        # Connect to Elasticsearch
        es = get_elastic_client("local")

        # Iterate over each doc and index it in Elasticsearch
        for doc in self.labeled_docs:
            # Index the document
            response = es.index(index=self.to_index, document=doc)
            
            # Optionally, you can check the response for success
            if response["result"] != "created":
                print(f"Failed to index document: {doc}")

        # Refresh the index to make the changes visible for search operations
        es.indices.refresh(index=self.to_index)
        self.next(self.end)


    @step
    def end(self):
        """Print results summary"""
        print(f"Labeled {len(self.labeled_docs)} docs in: {self.to_index}")

if __name__ == '__main__':
    flow = SfSplitDataset()
    flow.run()
