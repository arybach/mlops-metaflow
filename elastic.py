# cloud example
from elasticsearch import Elasticsearch
import os


def get_elastic_client(mode: str ='local'):
    # local or cloud

    ES_CLOUD_ID=os.environ.get("ES_CLOUD_ID")
    ES_USERNAME=os.environ.get("ES_USERNAME")
    ES_PASSWORD=os.environ.get("ES_PASSWORD")
    ES_ENDPOINT=os.environ.get("ES_ENDPOINT")
    #ES_LOCAL_HOST=os.environ.get("ES_LOCAL_HOST")
    ES_LOCAL_HOST="localhost"
    ES_CLOUD_HOST=os.environ.get("ES_CLOUD_HOST")

    # Create the Elasticsearch client
    if mode == 'cloud':
        # Connect to Elasticsearch cloud
        # port = 9243        
        client = Elasticsearch(
            cloud_id=ES_CLOUD_ID,
            http_auth=(ES_USERNAME, ES_PASSWORD)    
        )

    elif mode == 'local':

        if ES_LOCAL_HOST:
            local_host=ES_LOCAL_HOST
        else:
            local_host="elasticsearch"
            
        port = 9200
        scheme = "http"
        username = "elastic"
        password = "elasticsearchme"
        
        # Create the Elasticsearch client
        client = Elasticsearch(
            hosts=[{"host": local_host, "port": port, "scheme": scheme}],
            http_auth=(username, password)
        )
    else:
        client = None
    
    # if client:
        # Successful response!
        # print(client.cluster.health())
        
    return client


def drop_index(es_client, index):
    es_client.indices.delete(index=index)
    print(f"Index '{index}' has been dropped successfully.")
