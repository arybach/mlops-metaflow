import os, time
from botocore.exceptions import ClientError
from time import sleep
from metaflow import Flow, FlowSpec, step, retry, batch
from utils import download_from_s3, list_files_in_folder, upload_to_s3, read_data
from config import bucket_name, image

class SfUsdaFlow(FlowSpec):
    """
    A Metaflow flow for extracting frames from a video file in parallel using Dask.    
    """
    #@batch(cpu=1, memory=3500, image=image)
    @step
    def start(self):
        """Start the flow, init parameters, and get streams from a prior run."""

        self.bucket_name = bucket_name
        self.prefix = "nutrients"
        self.docs = [] # for pylint
        self.fields = [] # for pylint

        contents = list_files_in_folder(self.bucket_name, self.prefix)
        self.files = contents.get(self.prefix)
        self.next(self.download, foreach='files')


    #@batch(cpu=1, memory=3500, image=image)
    #@retry(times=3, minutes_between_retries=1)
    @step
    def download(self):

        print(self.input)
        self.filename = self.input
        self.bucket_name = bucket_name
        self.prefix = "nutrients"
        
        # check if the file already exists on S3
        if self.filename:
            self.s3_path = os.path.join(f's3://{self.bucket_name}', self.prefix, f'{self.filename}')

            # foreach is run on a new instance every time
            os.makedirs(os.path.join(self.bucket_name, self.prefix), exist_ok=True)

            file_path = download_from_s3(self.s3_path)
            self.docs = read_data([file_path], "nutrients")             

        self.next(self.join)

    #@batch(cpu=1, memory=3500, image=image)
    @step
    def join(self, inputs):
        """Join the results"""
        if not hasattr(self, 'docs'):
            self.docs = []

        for input in inputs:
            self.docs.extend(input.docs)  # Extend self.docs with the docs from each input

        self.next(self.end)

    #@batch(cpu=1, memory=3500, image=image)
    @step
    def end(self):
        """End the flow"""
        print(f"Fetched ${len(self.docs)} docs")

if __name__ == '__main__':
    flow = SfUsdaFlow()
    flow.run()

