from pymongo.mongo_client import MongoClient
import requests

model_id = "sentence-transformers/all-MiniLM-L6-v2"
hf_token = "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"


api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
headers = {"Authorization": f"Bearer {hf_token}"}

def query(search_query):
    response = requests.post(api_url, headers=headers, json={"inputs": search_query, "options":{"wait_for_model":True}})
    return response.json()


uri = "mongodb+srv://techabidallabib:L9QzyLDpkZYzdWDI@vectorclustertest.pxmtf.mongodb.net/?retryWrites=true&w=majority&appName=vectorClusterTest"
client = MongoClient(uri)

# Access your database and collection
database = client["sample_mflix"]
embedded_movies_collection = database["embedded_movies"]


search_query = "young  man in the sea meets a bengal tiger"
embedded_search = query(search_query)

pipeline = [
  {
    '$vectorSearch': {
      'index': 'vector_index', 
      'path': 'plot_embedding', 
      'queryVector': embedded_search,
      'numCandidates': 150, 
      'limit': 10
    }
  }, {
    '$project': {
      '_id': 0, 
      'plot': 1, 
      'title': 1, 
      'score': {
        '$meta': 'vectorSearchScore'
      }
    }
  }
]

# run pipeline
result = client["sample_mflix"]["embedded_movies"].aggregate(pipeline)
# print results
for i in result:
    print(i)
 