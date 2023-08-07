import pandas as pd
from elastic import get_elastic_client
import pyarrow as pa
import pyarrow.parquet as pq
from metaflow import Flow, FlowSpec, step, retry, batch
from utils import upload_to_s3
from config import bucket_name, index

class SfExportDocsToS3(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @step
    def start(self):
        """fetch docs from elastic search index"""

        # Connect to Elasticsearch
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration

        # Define the index name
        self.index_name = index
        scroll_size = 10000  # Adjust the scroll size based on your requirements

        # Define the query to retrieve all documents
        query = {
            "query": {
                "match_all": {}
            },
            "size": scroll_size  # Adjust the batch size based on your requirements
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

        # Convert the documents to a pandas DataFrame
        df = pd.DataFrame([doc["_source"] for doc in docs])
        # Check if 'food_nutrients' field is present and drop it if it's empty
        if 'food_nutrients' in df.columns and ('nutrients' in df.columns or 'labelNutrients' in df.columns):
            df = df.drop(columns=['food_nutrients'])
        else:
            print(df.columns)
    
        self.df = df

        self.next(self.save_to_s3)

    @step
    def save_to_s3(self):
        self.filename = f"{self.index_name}.parquet"
        self.bucket_name = bucket_name
        self.prefix = self.index_name

        local_path = f"{self.prefix}/{self.filename}"

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(self.df)

        # Write the table to Parquet
        pq.write_table(table, local_path)

        self.s3_path = upload_to_s3(local_path, self.bucket_name)
        self.next(self.end)

    @step
    def end(self):
        """Print results"""
        print(f"Uploaded {len(self.df)} docs to: {self.s3_path}")

if __name__ == '__main__':
    flow = SfExportDocsToS3()
    flow.run()
