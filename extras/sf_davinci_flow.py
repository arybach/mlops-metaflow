import os, time
from botocore.exceptions import ClientError
from time import sleep
from metaflow import Flow, FlowSpec, step, retry, batch
from utils import download_from_s3, list_files_in_folder, upload_to_s3, read_data
import boto3

class SfDavinciFlow(FlowSpec):
    """
    A Metaflow flow for extracting frames from a video file in parallel using Dask.    
    """
    #@batch(cpu=1, memory=3500, image=image)
    @step
    def start(self):
        """Start the flow, init parameters, and get streams from a prior run."""

        self.bucket_name = "usda.nutrients"
        self.prefix = "recipes"
        self.docs = [] # for pylint

        contents = list_files_in_folder(self.bucket_name, self.prefix)
        
        self.files = contents.get(self.prefix)

        # for testing set to first 3 files:
        # self.files = contents.get(self.prefix)[2:5]
        self.files = contents.get(self.prefix)

        self.next(self.download, foreach='files')


    #@batch(cpu=1, memory=3500, image=image)
    #@retry(times=3, minutes_between_retries=1)
    @step
    def download(self):
        self.filename = self.input
        self.bucket_name = "usda.nutrients"
        self.prefix = "recipes_data"
        self.docs = []
        self.modjson = []

        # Check if the file already exists on S3
        if self.filename and self.filename.endswith("_processed.json"):
            
            s3 = boto3.client('s3', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
            s3_path = os.path.join(f's3://{self.bucket_name}', self.prefix, f'{self.filename.replace("_processed.json", "_davinci.json")}')

            try:
                s3.head_object(Bucket=self.bucket_name, Key=s3_path)
                print(f"File {s3_path} already exists. Downloading docs.")
                file_path = download_from_s3(s3_path)
                self.docs = read_data([file_path], "recipes")             
                self.modjson = [s3_path]

            except ClientError:
                # File does not exist, proceed with download and processing
                s3_path = os.path.join(f's3://{self.bucket_name}', self.prefix, f'{self.filename}')

                os.makedirs(os.path.join(self.bucket_name, self.prefix), exist_ok=True)

                file_path = download_from_s3(s3_path)
                self.docs = read_data([file_path])             
                # this is expensive - only use when data is sparse and needs updating
                # self.docs, modified_jsonfile = jsonfile_to_doc([file_path])
                # self.modjson = [ upload_to_s3(modified_jsonfile, bucket_name) ]
        else:
            print("Skipping: ", self.filename)

        self.next(self.join)
    
    #@batch(cpu=1, memory=3500, image=image)
    @step
    def join(self, inputs):
        """Join the results"""
        if not hasattr(self, 'docs'):
            self.docs = []

        # if not hasattr(self, 'modjson'):
        #     self.modjson = []

        for input in inputs:
            self.docs.extend(input.docs)  # Extend self.docs with the docs from each input
            # self.modjson.extend(input.modjson)  # Extend self.docs with the docs from each input

        self.next(self.end)

    #@batch(cpu=1, memory=3500, image=image)
    @step
    def end(self):
        """End the flow"""
        print(f"Fetched {len(self.docs)} docs")


if __name__ == '__main__':
    flow = SfDavinciFlow()
    flow.run()

