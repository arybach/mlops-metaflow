### create new s3 bucket: mlops-nutrients (already created in ap-southeast-1) with the following 
### bucket access policy - DON"T FORGET TO SPECIFY YOUR OWN PRINCIPLE ACCOUNTS! to use a bucket for read/write.
### reading from mlops-nutrients bucket is allowed to anyone at the moment

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
                    "arn:aws:iam::388062344663:user/arybach"
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

### save all pre-processed docs from ES nutrients index to s3 using export_docs_to_s3.py
or its metaflow flow version:
sf_export_docs_to_s3_flow.py


### install conda
curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
conda --version

### as batch flows are run with --environment=conda (alternative is to build a custom image and run with @batch(image='image')
conda install -c conda-forge sentence-transformers