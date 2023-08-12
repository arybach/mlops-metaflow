## Introducing a Flavorful Experiment: A Repo Dedicated to Healthy Nutrition and More
    https://github.com/arybach/mlops-infra.git

    Development Stage Alert: This repository is currently in the development phase. It's my special pet project whipped up for my wife, Natalia, who happens to be an aficionado of healthy nutrition.

    Here's what's cooking:

    1. Taste-Testing Food Labeling:
    The main course of this project is to enable testing of various scoring models for food items. While we're using a mock-up scoring function for now (because hey, I'm not a data scientist!), the focus is on the MlOps/DevOps part.

    2. Extra Ingredients:
    Check out the snippet of code under extras, where a more complex approach to scoring awaits. Be mindful, though; experimenting with it could run upwards of $800 on API calls.

    3. Add Your Spice:
    This kitchen is open to all! Contribute your version of the scoring function or throw in model tests, reports, or additional integrations. Plus, it's all open-source.

    4. Serving a Side of Knowledge:
    This isn't just a project; it's a step-by-step introduction to Metaflow on AWS Batch, Step Functions, and integrations with WANDB and Evidently. It's tailored for those interested in Metaflow but facing onboarding obstacles.

    5. The Bill, Please:
    Fully deploying this dish takes around 3 to 5 hours, and it costs $300+ monthly, depending on compute time and resources. Fancy the Elastic Search enterprise license? That'll be an extra $200+ monthly.
    😀


### Create new s3 bucket: mlops-nutrients (already created in ap-southeast-1) with the following:
* bucket access policy - DON"T FORGET TO SPECIFY YOUR OWN PRINCIPLE ACCOUNTS! to use a bucket for read/write.
* reading from mlops-nutrients bucket is allowed to anyone at the moment
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowListAccess",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::mlops-nutrients"
        },
        {
            "Sid": "AllowReadAccess",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": "arn:aws:s3:::mlops-nutrients/*"
        },
        {
            "Sid": "AllowWriteAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::388062344663:user/awsworker",
                    "arn:aws:iam::388062344663:user/mlopsmetaflow"
                ]
            },
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:PutObjectAcl",
                "s3:DeleteObjectVersion",
                "s3:PutObjectVersionAcl"
            ],
            "Resource": "arn:aws:s3:::mlops-nutrients/*"
        }
    ]
}
```

### Make sure that all envs are exported either in the terminal or ~/.bashrs file - besides being added to vault, region should be without quotes
```export AWS_ACCESS_KEY_ID="XXXXXXXXXXXXXXXXX"```
```export AWS_SECRET_ACCESS_KEY="XXXXXXXXXXXXXXXXXX"```
```export AWS_REGION=ap-southeast-1```
```export OPENAI_API_KEY="XXXXXXXXXXXXXXXXXXXXXXXXXX"```
```export HUGGING_FACE_TOKEN="XXXXXXXXXXXXXXXXXXXXXXXXXXXX"```
```export ES_PASSWORD="XXXXXXXXXXXXXXXXXXXXXXXX"```
```export USDA_API_KEY="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"```
```export WANDB_API_KEY="XXXXXXXXXXXXXXXXXXXXXXXXXXX"```

### in the config.py file specify your own s3 bucket name, es index, model name, es host (local or cloud - by default local is used. search and replace to change for cloud, but additional en var will be needed) 

    ```
    bucket_name='mlops-nutrients'
    index='nutrients'
    model="msmarco"
    es_local_host="18.143.34.247"
    es_cloud_host="elastic.es.us-central1.gcp.cloud.es.io"
    ```

### Run extra flows (not-required):
* ### to fetch pre-processed data from s3 and save as metaflow artifacts
    `python3 batch_usda_flow.py run`
* ### to fetch archived .parquet file from mlops-nutrients bucket and add enrich docs missing from the elasticserach index with direct calls to usda api (VERY SLOW due to api rate limits!!! - can be interrupted at any time and resumed later)
    `python3 batch_usda_to_es_flow.py run`
* ### export docs from nutrients index to s3://mlops-nutrients/nutrients/nutrients.parquet (only need to run after batch_usda_to_es_flow.py)
    `python3 sf_export_docs_to_s3_flow.py run`

### Run required flows:
* ### to fetch all archived docs from s3://mlops-nutrients/nutrients/nutrients.parquet - label as training, validation and testing and save to labeled index in elsaticsearch 
    `python3 sf_split_dataset_flow.py run`
* ### to clusterize all docs in labeled index and save models to s3://mlops-nutrients/models
    `python3 sf_es_clusters_flow.py run`
### the card saves details of the model run - run command below to view in the browser (clusters.html file contains an example output):
    `python3 sf_es_clusters_flow.py card view visualize_training`
    `python3 sf_es_clusters_flow.py card view validate`
### card view step_name command is needed to view each step in the flow (in this case WANDB is a bit more comvinient) 

* ### to run linear regression model on the same data use:
    `python3 sf_es_lr_flow.py run`
### the card saves details of the model run - run command below to view in the browser (lr.html file contains an example output):
    `python3 sf_es_lr_flow.py card view visualize`

### WANDB can also be used to track lineage, version data and models and keep track of the experiments. To use it, specify export WANDB_API_KEY and add WANDB_PROJECT and WANDB_ENTITY in the config.py file and run the following command:
    `python3 sf_wandb_es_clusters_flow.py run`

    ``` 
    sample output below:
    2023-07-20 09:45:11.036 [44/end/219 (pid 46219)] wandb: Syncing run cerulean-pyramid-5
    2023-07-20 09:45:15.349 [44/end/219 (pid 46219)] wandb: ⭐️ View project at https://wandb.ai/tumblebuns/mlops-metaflow
    2023-07-20 09:45:15.350 [44/end/219 (pid 46219)] wandb: 🚀 View run at https://wandb.ai/tumblebuns/mlops-metaflow/runs/xh97z80l
    2023-07-20 09:45:15.350 [44/end/219 (pid 46219)] wandb: Using artifact: clustering_hierarchical (<class 'sklearn.cluster._agglomerative.AgglomerativeClustering'>)
    2023-07-20 09:45:16.831 [44/end/219 (pid 46219)] wandb: Using artifact: clustering_cmeans (<class 'sklearn.cluster._kmeans.KMeans'>)
    2023-07-20 09:45:16.843 [44/end/219 (pid 46219)] wandb: Waiting for W&B process to finish... (success).
    2023-07-20 09:45:25.794 [44/end/219 (pid 46219)] wandb:
    2023-07-20 09:45:25.799 [44/end/219 (pid 46219)] wandb: Run summary:
    2023-07-20 09:45:25.799 [44/end/219 (pid 46219)] wandb:                cmeans_path models/cmeans_cluste...
    2023-07-20 09:45:25.800 [44/end/219 (pid 46219)] wandb:             hierarchy_path models/hierarchical_...
    2023-07-20 09:45:25.800 [44/end/219 (pid 46219)] wandb:       path_to_cmeans_model s3://mlops-nutrients...
    2023-07-20 09:45:25.800 [44/end/219 (pid 46219)] wandb: path_to_hierarchical_model s3://mlops-nutrients...
    2023-07-20 09:45:25.800 [44/end/219 (pid 46219)] wandb:
    2023-07-20 09:45:25.800 [44/end/219 (pid 46219)] wandb: 🚀 View run cerulean-pyramid-5 at: https://wandb.ai/tumblebuns/mlops-metaflow/runs/xh97z80l
    2023-07-20 09:45:30.715 [44/end/219 (pid 46219)] wandb: Synced 5 W&B file(s), 0 media file(s), 2 artifact file(s) and 0 other file(s)
    2023-07-20 09:45:30.715 [44/end/219 (pid 46219)] wandb: Find logs at: ./wandb/run-20230720_094509-xh97z80l/logs
    ```

### in WANDB GUI we are mostly interested in the artifacts and the models since deploying models can be done via WANDB (although the latest model is saved in the s3 bucket - no versioning has been set up for the s3 bucket. Metaflow automatically saves all self. variables as artifacts, so models are versioned with the run ids)):
![Alt text](image-2.png)

### plotting results (manual customization in the GUI compared to metaflow cards - programmatic visualization options are more difficult)
![Alt text](image-1.png)

### for WANDB lineage to work properly all steps from different flows would need to be merged into a single file (which is fine at the end, but not during development) 

### adding WANDB to sf_wandb_lr_flow.py
    `python3 sf_wandb_lr_flow.py run`

### system monitoring plots
![Alt text](image-3.png)

### yet getting WANDB to plot residuals is actually more challenging than in metaflow as it has to be done manually in the UI
![Alt text](image-4.png)

### WANDB stashes copy of the actual dataset, lineage and the dependencies used (this + model versioning and deployment is more important to me than its visualization capabilities)
![Alt text](image.png)

### run xgboost model on the same data use:
    `python3 sf_es_xgboost_flow.py run`
### to see xgboost card and validation visualizations woth metaflow run:
    `python3 sf_es_xgboost_flow.py card view xgboost_regression`
    `python3 sf_es_xgboost_flow.py card view visualize`

### and the WANDB version - passing plots from flows to WANDB as artifacts makes them less useful than the interactive plots prepared in WANDB GUI, so doesn't make sense (so logging only datasets and models). Defining custom dataframe and then logging them to wandb as wandb.table() for visualization in wandb gui is more efficient
    `python3 sf_wandb_es_xgboost_flow.py run`

### to introduce data drift - login into kibana and under Management - Index - delete nutrients index and run (overnight to fetch sufficient quantity of new docs):
    `python3 batch_usda_to_es_flow.py run`
    `python3 sf_es_drift_flow.py run`


### to RUN batch flows:
* ### to run parallelized flows on aws batch the command is different (@conda_base has to be defined for Class or on a step level)
    `CONDA_CHANNELS=conda-forge python3 sf_xxxx_flow.py --environment=conda run`

### it requires conda to run, hence:
    `wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh`
    `bash miniconda.sh -b -p $HOME/miniconda`
    `echo 'export PATH=$HOME/miniconda/bin:$PATH' >> ~/.bashrc`
    `source ~/.bashrc`
    `rm miniconda.sh`

### as batch flows are run with --environment=conda (alternative is to build a custom image and run with @batch(image='image')) install all conda dependencies
    `conda create -n batch python=3.10`
    `conda activate batch`
    `conda config --add channels conda-forge`

### instead of standard @conda_base i find it easier to decorate steps with specs @batch(cpu=2, memory=3500,image=image), where image is a link to ECR image built in batch terraform module
    `python3 sf_embeddings_flow.py run --max-workers 5`

to view reports run flows:
    `python3 sf_es_evidently_flow.py run`

and then view cards:
    'python3 sf_es_evidently_flow.py card view monitoring_data_quality'
evidently sample data quality report saved under monitoring_data_quality.html
    'python3 sf_es_evidently_flow.py card view data_drift_test'
evidently sample data drift report saved under data_drift_report.html
    'python3 sf_es_evidently_flow.py card view xgboost_model_performance'
evidently sample xgboost model performance report saved under xgboost_model_performance.html
    'python3 sf_es_evidently_flow.py card view regression_model_performance'
evidently sample regression model performance report saved under regression_model_performance.html

### metaflow cards are also available directly in s3 (open to view):
s3://metaflow-s3-mlops-apse1/metaflow/mf.cards/

alternatively after terraform fastapi model has been deployed we can fetch predictions via fastapi and write data to postgresdb to generate a rolling-window reports (instead of a specific batch in the above example)
    'python3 sf_es_evidently_fastapi_flow.py run'

and then view cards (these ones fetched via fastapi):
    'python3 sf_es_evidently_fastapi_flow.py card view regression_model_performance'
    'python3 sf_es_evidently_fastapi_flow.py card view xgboost_model_performance'
    'python3 sf_es_evidently_fastapi_flow.py card view regression_target_monitoring'
    'python3 sf_es_evidently_fastapi_flow.py card view xgboost_target_monitoring'


### Deploying flows to Step Functions (after @batch decorators are set for each task and the flows are tested) - we need to use sf_usda_to_es_flow.py flow as accessing artifacts from another flow fails in Step Functions
    'python3 batch_usda_flow.py --with retry step-functions create'
    'python3 sf_split_dataset_flow.py --with retry step-functions create'    
    'python3 sf_embeddings_flow.py --with retry step-functions create'
### when deploying an updated version - delete previous one as it doesn't update automatically

### now we can test run these flows in this order using AWS Step Functions console
first run the script:
'''
from elastic import get_elastic_client, drop_index

es_client = get_elastic_client('local')
drop_index(es_client, 'nutrients')
drop_index(es_client, 'labeled')
exit()
'''
[0] % python3
Python 3.10.12 (main, Jun 11 2023, 05:26:28) [GCC 11.4.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from elastic import get_elastic_client, drop_index
ex(es_client, 'nutrients')
drop_index(es_client, 'labeled')
>>> 
>>> es_client = get_elastic_client('local')
SecurityWarning: Connecting to 'https://18.142.82.36:9200' using TLS with verify_certs=False is insecure
  _transport = transport_class(
>>> drop_index(es_client, 'nutrients')
Index 'nutrients' has been dropped successfully.
>>> drop_index(es_client, 'labeled')
Index 'labeled' has been dropped successfully.
>>> exit()

### choose a flow and click on Start Execution
![Alt text](image-5.png)
![Alt text](image-6.png)

then run remaining scripts..
Metaflow deployment with Argo supports @trigger on finish() aws batch - doesn't support that yet
instead fetch a list of states machines deployed to aws:
'aws stepfunctions list-state-machines --query 'stateMachines[*].stateMachineArn'
it will look like this:
'''
[
    "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfEmbeddingsFlow",
    "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfSplitDataset",
    "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfUsdaToEsFlow"
]
'''
and copy the role arn (it might need additional permissions for non default project buckets and kms keys)
'arn:aws:iam::388062344663:role/metaflow-step_functions_role-mlops-apse1'

then update run_state_machines.py and run it to execute flows sequentially
by starting ChainedUsdaFlow in the AWS Step Functions console
![Alt text](image-7.png)

After two indecies have been created - nutrients with embeddings and lebeled (with 3 way split into training, validation and testing)
we can run flows with models, tests and cards in batch mode. 
'python3 sf_es_clusters_flow.py run'
'python3 sf_es_lr_flow.py run'
'python3 sf_es_xgboost_flow.py run'

metaflow_batch
and metaflow_sf roles will need additional permissions if using a separate bucket from project's default. 
These can be modified under IAM roles -> type metaflow and u[date KMS and S3 allowed actions + resources
Alternatively git clone entire metaflow repo with terraform for AWS Batch and modified policy definitions in the code before terraform apply
create Step Functions
'python3 sf_es_clusters_flow.py --with retry step-functions create'
'python3 sf_es_lr_flow.py --with retry step-functions create'
'python3 sf_es_xgboost_flow.py --with retry step-functions create'

Same flows using WANDB to store artifacts in addition to Metaflow own storage
'python3 sf_wandb_es_clusters_flow.py run'
'python3 sf_wandb_es_lr_flow.py run'
'python3 sf_wand_es_xgboost_flow.py run'

create Step Functions
'python3 sf_wandb_es_clusters_flow.py --with retry step-functions create'
'python3 sf_wandb_es_lr_flow.py --with retry step-functions create'
'python3 sf_wandb_es_xgboost_flow.py --with retry step-functions create'

Now we can run tests on data quality, data drift and check models preformance with Evidently
Update default value of the 'MONITORING_DB_HOST' in the config.py from '0.0.0.0' to the IP address of the fastapi host (terraform output)
'fastapi_host: Text = os.getenv('MONITORING_DB_HOST','13.229.1.117')'
then run 
'python3 sf_es_evidently_flow.py run'
'python3 sf_es_evidently_fastapi_flow.py run'
create Step Functions
'python3 sf_es_evidently_flow.py --with retry step-functions create'
'python3 sf_es_evidently_fastapi_flow.py --with retry step-functions create'

again, we can check the cards (this time generated using AWS Batch):
    'python3 sf_es_evidently_fastapi_flow.py card view monitoring_data_quality'
>>>  mlops-metaflow/evidently_monitoring_data_quality.html
    'python3 sf_es_evidently_fastapi_flow.py card view data_drift_test'
>>>  mlops-metaflow/evidently_data_drift_test.html

and these - fetched via fastapi:
    'python3 sf_es_evidently_fastapi_flow.py card view regression_model_performance'
>>>  mlops-metaflow/evidently_regression_model_performance.html
    'python3 sf_es_evidently_fastapi_flow.py card view xgboost_model_performance'
>>>  mlops-metaflow/evidently_xgboost_model_performance.html
    'python3 sf_es_evidently_fastapi_flow.py card view regression_target_monitoring'
>>>  mlops-metaflow/evidently_regression_target_performance.html
    'python3 sf_es_evidently_fastapi_flow.py card view xgboost_target_monitoring'
>>>  mlops-metaflow/evidently_xgboost_target_performance.html

add arns to the run_state_machine.py to then trigger all flows at once in the AWS Step Functions console

### Note: AWS Batch jobs can get stuck in Runnable state when there are no available resources (EC2 instances with the right AMI, VCPUs and RAM configurations) - easiest solution is to expand types of EC2 instances used by the queue in the metaflow Terraform module variables.tf file. Although that would require to destroy and re-create the entire infrastructure after infra step (so Metaflow, EC2, Fastapi and Batch). Anothe

### Model deployment

WANDB - register model in UI:
![Alt text](image-8.png)

.github/workflows/deploy_model.yml
will deploy xgboost model to AWS Lambda from WANDB registry and linear regression model from Metfalow artifcats (as an example)

Adding versioning to mlops-nutrients/model bucket would make this more robust