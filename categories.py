import nltk
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from elasticsearch import Elasticsearch
from elastic import get_elastic_client
from subs import veggies, mushrooms, nuts, seeds, berries, beans, fruits, peppers, squash, grains, wheat, rice, melons, seaweeds, oils, herbs, foodstock, odd_measures, ingredients_to_remove

## Download necessary NLTK resources (if not already downloaded)
#nltk.download("punkt")
#nltk.download("averaged_perceptron_tagger")
#nltk.download("brown")
#nltk.download('wordnet')
#from nltk.corpus import wordnet as wn
## NLP toolkit - retrieve all food-related stop words
#def get_nltk_food_list()->list:
#    food = wn.synset('food.n.02')
#    fl = list(set([w for s in food.closure(lambda s:s.hyponyms()) for w in s.lemma_names()]))
#    return fl    
##    http://vegetablesfruitsgrains.com/list-of-vegetables/
##    http://edis.ifas.ufl.edu/features/fruitvegindex.html
##    http://www.enchantedlearning.com/wordlist/vegetables.shtml


# Define food categories
food_categories = {
    "fruits": fruits,
    "vegetables": veggies,
    "nuts": nuts,
    "seeds": seeds,
    "berries": seeds,
    "beans": beans,
    "mushrooms": mushrooms,
    "peppers": peppers,
    "squash": peppers,
    "grains": grains,
    "wheat": wheat,
    "rice": rice,
    "melons": melons,
    "seaweeds": melons,
    "oils": oils,
    "herbs": herbs,
    "foodstock": foodstock,
    # Add more categories as needed
}

# Initialize the Elasticsearch client
es = get_elastic_client("local")

def get_vector(index_w_ve, token):
    # Retrieve the vector embedding for the given token from the Elasticsearch index
    result = es.search(
        index=index_w_ve,
        body={
            "query": {"term": {"description.keyword": token}},
            "_source": ["vector_embeddings"]
        }
    )
    if result["hits"]["total"]["value"] > 0:
        vector = result["hits"]["hits"][0]["_source"]["vector_embeddings"]
        return vector
    else:
        return None

    
def get_category_embeddings(index_name):
    # Preprocess the food categories to generate vector embeddings
    category_embeddings = {}
    for category, keywords in food_categories.items():
        category_tokens = nltk.word_tokenize(" ".join(keywords).lower())
        category_embeddings[category] = np.mean([get_vector(index_name, token) for token in category_tokens], axis=0)
        
    return category_embeddings


# Function to categorize a description based on NLTK food categories
def categorize_description(index_name, description):
    
    category_embeddings = get_category_embeddings(index_name)
    tokens = nltk.word_tokenize(description.lower())
    description_embedding = np.mean([get_vector(index_name, token) for token in tokens], axis=0)  # Calculate the description embedding
    similarities = cosine_similarity([description_embedding], list(category_embeddings.values()))[0]  # Calculate cosine similarities
    categories = [list(category_embeddings.keys())[index] for index in np.argsort(similarities)[::-1]]  # Sort categories by similarity
    
    return categories

# Function to update documents in Elasticsearch with category field and return list of documents with no category
def update_documents_with_category(index_name):
    # Retrieve documents from the Elasticsearch index
    docs = es.search(index=index_name, body={"query": {"match_all": {}}})["hits"]["hits"]

    # Update documents in Elasticsearch with the category field
    for doc in docs:
        try:
            description = doc['_source']["description"]
            categories = categorize_description(index_name, description)
            best_category = categories[0]  # Get the best matching category
            es.update(index=index_name, id=doc["_id"], body={"doc": {"category": best_category}})
        except KeyError:
            print("Error processing document:")
            print(doc)

    # Retrieve updated documents from the Elasticsearch index
    updated_docs = es.search(index=index_name, body={"query": {"match_all": {}}})["hits"]["hits"]

    # Return the list of documents with no category
    documents_with_no_category = []
    for doc in updated_docs:
        if not doc.get("_source", {}).get("category"):
            documents_with_no_category.append(doc)

    return documents_with_no_category
