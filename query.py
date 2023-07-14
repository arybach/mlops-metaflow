import requests
import elastic
from requests.auth import HTTPBasicAuth

# Set the Elasticsearch endpoint and API key information (for access from ec2 instance where elasticsearch docker container is running)
# from ../.sectrets/api_keys get ...
elasticsearch_url = 'http://localhost:9200'
api_key_name = '***'
api_key_value = '****'
username = 'elastic'
password = 'elasticsearchme'
index_name = 'nutrients'
usda_api_key = '*****'

def get_food_info(fdcid, api_key=usda_api_key):
    url = f"https://api.nal.usda.gov/fdc/v1/food/{fdcid}?nutrients=203&nutrients=204&nutrients=205&api_key={api_key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None
    
def execute_post_query(query, elasticsearch_url=elasticsearch_url, api_key_name=api_key_name, api_key_value=api_key_value, username=username, password=password, index_name=index_name):
    # Set the headers with API key and basic authentication
    headers = {
        'Authorization': f'ApiKey {api_key_name}:{api_key_value}',
        'Content-Type': 'application/json'
    }

    # Make the search request with basic authentication
    response = requests.post(
        f'{elasticsearch_url}/{index_name}/_search?search_type=query_then_fetch',
        auth=HTTPBasicAuth(username, password),
        headers=headers,
        json=query
    )

    # Check the response
    if response.status_code == 200:
        # Process the response data
        response_data = response.json()
        # Do something with the response data
        # Retrieve the aggregations
        aggregations = response_data['aggregations']
        return aggregations
    else:
        print(f'Request failed with status code: {response.status_code}')
        print(response.text)
        return None
    

def execute_get_query(query, elasticsearch_url=elasticsearch_url, api_key_name=api_key_name, api_key_value=api_key_value, username=username, password=password, index_name=index_name):
    # Set the headers with API key and basic authentication
    headers = {
        'Authorization': f'ApiKey {api_key_name}:{api_key_value}',
        'Content-Type': 'application/json'
    }

    # Make the search request with basic authentication
    response = requests.get(
        f'{elasticsearch_url}/{index_name}/_search',
        auth=HTTPBasicAuth(username, password),
        headers=headers,
        json=query
    )

    # Check the response
    if response.status_code == 200:
        # Process the response data
        response_data = response.json()
        # Do something with the response data
        # Retrieve the documents
        hits = response_data['hits']['hits']
 
        return hits
    else:
        print(f'Request failed with status code: {response.status_code}')
        print(response.text)
        return None

# # Set the search query
# search_query = {
#     'query': {
#         'match_all': {}
#     }
# }

# # Set the query
# search_query = {
#   "_source": {
#     "includes": ["food_nutrients.*"]
#   },
#   "query": {
#     "match_all": {}
#   }
# }

# Set the aggregation query to find the minimum and maximum score values
aggregation_query = {
    "aggs": {
        "min_score": {
            "min": {
                "field": "score"
            }
        },
        "max_score": {
            "max": {
                "field": "score"
            }
        },
        "zero_score_docs": {
            "terms": {
                "field": "score",
                "size": 1,
                "min_doc_count": 1,
                "order": {
                    "_count": "desc"
                }
            }
        },
        "highest_score_docs": {
            "top_hits": {
                "size": 2,
                "sort": [
                    {"score": {"order": "desc"}}
                ]
            }
        },
        "lowest_score_docs": {
            "top_hits": {
                "size": 2,
                "sort": [
                    {"score": {"order": "asc"}}
                ]
            }
        }
    }
}
# for aggregation use post query
#aggregation = execute_post_query(aggregation_query)
#print(aggregation)
#hits = execute_get_query(aggregation_query)
#print(hits)
fdcid = "1160157"
food_info = get_food_info(fdcid)
print(food_info.get("labelNutrients"))

# all fields:
# 'id': The unique identifier of the food item.
# 'fid': The unique identifier of the food item.
# 'fdc_id': The unique identifier of the food data central.
# 'description': The description or name of the food item.
# 'food_nutrients': A dictionary containing various nutrient information for the food item.

# 'food_nutrients' dictionary:
# 'Protein': The amount of protein in grams (g).
# 'Total lipid (fat)': The amount of total fat in grams (g).
# 'Carbohydrate, by difference': The amount of carbohydrates in grams (g).
# 'Energy': The amount of energy in kilocalories (kcal).
# 'Sugars, total including NLEA': The amount of total sugars in grams (g).
# 'Fiber, total dietary': The amount of dietary fiber in grams (g).
# 'Calcium, Ca': The amount of calcium in milligrams (mg).
# 'Iron, Fe': The amount of iron in milligrams (mg).
# 'Potassium, K': The amount of potassium in milligrams (mg).
# 'Sodium, Na': The amount of sodium in milligrams (mg).
# 'Vitamin D (D2 + D3), International Units': The amount of vitamin D in International Units (IU).
# 'Sugars, added': The amount of added sugars in grams (g).
# 'Cholesterol': The amount of cholesterol in milligrams (mg).
# 'Fatty acids, total trans': The amount of trans fat in grams (g).
# 'Fatty acids, total saturated': The amount of saturated fat in grams (g).
