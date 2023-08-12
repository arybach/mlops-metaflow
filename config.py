import os
from typing import Dict, Text
import sys  # Import the sys module
# Check if .env file exists before loading the environment variables
if os.path.isfile(".env"):
    import dotenv
    dotenv.load_dotenv(".env")

bucket_name='mlops-nutrients'
index='nutrients'
# model from hugging face used for vector embeddings - if any 
model="msmarco"
# set to ip address of ec2 instance created in mlops-infra/ec2
es_local_host=os.environ.get("ES_LOCAL_HOST", 'localhost')
es_cloud_host=''
es_password=os.environ.get("ES_PASSWORD","elasticsearchme")

# this is an example of a cloud hosted elasticsearch instance name
image="388062344663.dkr.ecr.ap-southeast-1.amazonaws.com/metaflow-batch-mlops-apse1:latest"
WANDB_ENTITY="tumblebuns"
WANDB_PROJECT="nutrients"

# fastapi_host is a public ip (if running metaflow flows from local machine) or private ip (if running in @batch mode) of the ec2-instance created by fastapi/terraform apply
# set host to fastapi ip and add default POSTGRES credentials if not set in Vault and ECR batch image 
fastapi_host: Text = os.getenv('MONITORING_DB_HOST','0.0.0.0')
database_user: Text = os.getenv('POSTGRES_USER', "changeme")
database_password: Text = os.getenv('POSTGRES_PASSWORD', "changeme")

# Use the environment variables to construct the DATABASE_URI
DATABASE_URI: Text = f'postgresql://{database_user}:{database_password}@{fastapi_host}:5432/monitoring_db'

# order of columns in num_features is important for linear regression model
DATA_COLUMNS: Dict = {
    'target_col': 'score',
    'prediction_col': 'predictions',
    'num_features': [
        "fat", "saturatedFat", "transFat", "cholesterol", "sodium",
        "carbohydrates", "fiber", "sugars", "protein", "calcium",
        "iron", "potassium", "calories"
    ],
    'cat_features': []
}
DATA_COLUMNS['columns'] = (
    DATA_COLUMNS['num_features'] +
    DATA_COLUMNS['cat_features'] +
    [DATA_COLUMNS['target_col'], DATA_COLUMNS['prediction_col']]
)
