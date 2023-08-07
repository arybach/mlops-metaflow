from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
from elastic import get_elastic_client

# example:
# https://www.elastic.co/guide/en/machine-learning/8.8/ml-nlp-model-ref.html#ml-nlp-model-ref-ner
# sentence transformers pretrained models summary: 
# https://www.sbert.net/docs/pretrained_models.html
# most efficient one for our use case: 
# MS MARCO is a large scale information retrieval corpus that was created based on real user search queries using Bing search engine
# for sentence prediction - large Q&A model
# sentence-transformers/bert-large-uncased-whole-word-masking-finetuned-squad
# 1024 dimensions, max sequence 384 (1-2 pages of text of varying density) - top accuracy scores

# best mini models for testing:
# fastest - all-MiniLM-L6-v2
# all around - msmarco-MiniLM-L-12-v3

# Define the models and their corresponding fields
models = {
    "msmarco": "sentence-transformers/msmarco-MiniLM-L-12-v3",
    "minilm": "sentence-transformers/all-MiniLM-L6-v2",
    "distilroberta": "sentence-transformers/all-distilroberta-v1",
    "mpnetbase": "sentence-transformers/all-mpnet-base-v2"
}


def get_model(model_name):
    """ Initialize the SentenceTransformer model """
    return SentenceTransformer(model_name)


def calculate_vector_embeddings(text, model_name):
    """ Calculate vector embeddings for the descriptions using a specific model """
    model = get_model(model_name)
    embeddings = model.encode(text)
    return embeddings.tolist()


def ingest_embeddings_to_elasticsearch(embeddings, index_name, doc_ids):
    """ Ingest the embedding vectors back into the Elasticsearch index """
    
    # Connect to either local or cloud client
    es = get_elastic_client("local")  
    for doc_id, embedding in zip(doc_ids, embeddings):
        body = {
            "doc": {
                "embedding": embedding
            }
        }
        try:
            # Fetch the document using the 'id' field
            result = es.search(index=index_name, body={"query": {"term": {"id": doc_id}}})
            
            # Check if any documents were found
            if result["hits"]["total"]["value"] > 0:
                # Get the '_id' of the first matching document
                doc_id = result["hits"]["hits"][0]["_id"]
                
                # Update the document using the '_id' field
                es.update(index=index_name, id=doc_id, body=body)
            else:
                print(f"No document found with id={doc_id} in index={index_name}")
        except Exception as e:
            print(f"Failed to update document with id={doc_id} in index={index_name}. Error: {e}")
            print("Arguments:")
            print(f"index_name: {index_name}")
            print(f"doc_id: {doc_id}")
            print(f"body: {body}")
            raise e  # Re-raise the exception
