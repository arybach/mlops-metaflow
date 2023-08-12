import wandb
import boto3
from metaflow import Flow
from config import bucket_name
import os

wandb.login()

run = wandb.init(project="nutrients", entity="tumblebuns")
artifact = run.use_artifact("wandb_xgboost_model:latest")
wandb_artifact_dir = artifact.download(root=".")

# Create an S3 client
s3 = boto3.client('s3')

# Handling WANDB xgboost model
wandb_model_local_path = os.path.join(wandb_artifact_dir, 'xgboost_model_wandb.joblib')
wandb_destination_path = 'models/xgboost_model.joblib'
s3.upload_file(wandb_model_local_path, bucket_name, wandb_destination_path)

# Handling Metaflow linear regression model
download_flow_res = Flow("SfEsLrFlow")
s3_lr_model_path = download_flow_res.latest_run["end"].task.data.path_to_regression_model
metaflow_local_path = "./linear_regression_model_metaflow.joblib"

# Download the linear regression model from S3
s3.download_file(bucket_name, s3_lr_model_path, metaflow_local_path)

# Upload path for linear regression model
upload_metaflow_destination_path = 'models/linear_regression_model.joblib'

# Upload the linear regression model file to S3
s3.upload_file(metaflow_local_path, bucket_name, upload_metaflow_destination_path)

# Clean up local files if needed
os.remove(wandb_model_local_path)
os.remove(metaflow_local_path)
