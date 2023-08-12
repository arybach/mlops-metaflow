import boto3
import os
import re, json
import pandas as pd
from daily_values import fix_recipes_labels, fix_nutrition_labels

AWS_ACCESS_KEY_ID=os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY")

##############################################################################################

def filename_from_s3_path(s3_path):
    """ returns path to filename from s3 path """
    s3_components = s3_path.split('/')
    bucket_name = s3_components[2]
    key = '/'.join(s3_components[3:])
    return key

##############################################################################################

def download_from_s3(s3_path):
    """
    Downloads a file from S3 to a local path and returns the local path.
    :param s3_path: str, the S3 path to the file to download
    :return: str, the local path where the file was downloaded to
    """
    # Split the S3 path into bucket and key components
    s3_components = s3_path.split('/')
    bucket_name = s3_components[2]
    key = '/'.join(s3_components[3:])

    # keeping local file structure the same as in S3
    local_dir = os.path.dirname(key)

    print(key)
    print(local_dir)
    # Create the local directory if it does not exist
    os.makedirs(local_dir, exist_ok=True)

    # Extract the local file name from the key and join it to the local directory
    local_file = os.path.join(local_dir, os.path.basename(key))

    # Download the file from S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.download_file(bucket_name, key, local_file)

    # Return the local path where the file was downloaded to
    return local_file
    
##############################################################################################

def upload_to_s3(local_path, bucket_name):
    """
    Uploads a file to S3 and returns the S3 path where the file was uploaded to.
    :param local_path: str, the local path to the file to upload
    :param bucket_name: str, the name of the S3 bucket to upload the file to
    :param aws_access_key_id: str, the AWS access key ID to use for authentication
    :param aws_secret_access_key: str, the AWS secret access key to use for authentication
    :return: str, the S3 path where the file was uploaded to
    """
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # s3 = boto3.resource('s3')
    # local_path should be w/o / at the beginning
    s3_path = f's3://{bucket_name}/{local_path}'
    s3.Object(bucket_name, local_path).upload_file(local_path)
    return s3_path

##############################################################################################

def list_files_in_folder(bucket_name, folder_path):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_path,
        Delimiter='/'
    )

    folders = {}
    for common_prefix in response.get('CommonPrefixes', []):
        folder_name = common_prefix['Prefix'].split('/')[-2]
        folders[folder_name] = []

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=common_prefix['Prefix']
        )

        for obj in response['Contents']:
            file_name = obj['Key'].split('/')[-1]
            folders[folder_name].append(file_name)

    return folders

##############################################################################################

# Download files from S3 into the ./data folder
def download_files_from_s3(s3_bucket, s3_object_key):
    """
    Downloads files from S3 to a local directory and returns the list of downloaded files.
    :param s3_bucket: str, the name of the S3 bucket
    :param s3_object_key: str, the object key or prefix to filter files in the bucket
    :return: list[str], the list of downloaded file paths
    """
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_object_key)
    filenames = []

    for obj in response['Contents']:
        file_key = obj['Key']
        filename = os.path.basename(file_key)
        if filename:
            local_file = os.path.join(s3_object_key, filename)
            s3.download_file(s3_bucket, file_key, local_file)
            filenames.append(local_file)

    return filenames

##############################################################################################

def read_data(file_paths, tag='recipes'):
    """ read nutrients and recipes jsonnl files into docs """
    data = []
    for file_path in file_paths:
        with open(file_path, "r") as file:
            for line in file:
                line = line.replace("\\\\", "\\")  # Replace "\\" with "\"
                line = re.sub(r'\\(?![/u"])', r"\\\\", line)  # Replace invalid escape sequences
                doc = json.loads(line.replace('=',':'))

                if doc.get("nutrients") or doc.get("food_nutrients"):
                    if tag == "recipes":
                        nutrients = fix_recipes_labels(doc.get("nutrients"))
                    elif tag == "nutrients":
                        nutrients = fix_nutrition_labels(doc.get("food_nutrients"))

                    doc["nutrients"] = nutrients  
                    # doc["nutrients"] = flatten_dicts_to_text(nutrients)
                else:
                    print("missing nutrients: ", doc)
                    doc["nutrients"] = ""
                
                if doc.get("ingredients"):
                    doc["ingredients"] = flatten_dicts_to_text(doc.get("ingredients"))
                elif tag != 'nutrients':
                    print("missing ingredients: ", doc)
                    doc["ingredients"] = ""

                if doc.get("directions"):
                    doc["directions"] = flatten_dicts_to_text(doc.get("directions"))
                elif tag != 'nutrients':
                    # print("missing directions: ", doc)
                    doc["directions"] = ""

                if doc.get("tips"):
                    doc["tips"] = flatten_dicts_to_text(doc.get("tips"))
                elif tag != 'nutrients':
                    doc["tips"] = ""

                data.append(doc)

    df = pd.DataFrame(data)
    df = df.rename(columns={"_id": "id"})
    return df.to_dict(orient="records")

##############################################################################################

def get_evidently_html(evidently_object) -> str:
    """Returns the rendered EvidentlyAI report/metric as HTML

    Should be assigned to `self.html`, installing `metaflow-card-html` to be rendered
    """
    import tempfile

    with tempfile.NamedTemporaryFile() as tmp:
        evidently_object.save_html(tmp.name)
        with open(tmp.name) as fh:
            return fh.read()
        
##############################################################################################

def flatten_dicts_to_text(dictionary):
    text = ""
    for field, val in dictionary.items():
        if isinstance(val, dict):
            subtext = flatten_dicts_to_text(val)
            text += f"{field}: {subtext}"
        else:
            text += f"{field}: {val}/n"
    return text

