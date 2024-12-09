
from pymongo.mongo_client import MongoClient
from pymongo.operations import SearchIndexModel
import time

# Connect to your Atlas deployment
uri = "mongodb+srv://techabidallabib:L9QzyLDpkZYzdWDI@vectorclustertest.pxmtf.mongodb.net/?retryWrites=true&w=majority&appName=vectorClusterTest"
client = MongoClient(uri)

# Access your database and collection
database = client["sample_mflix"]
collection = database["embedded_movies"]

fts_index = SearchIndexModel(
    definition={
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": [{
                    "type": "string",
                }]
            }
        }

    },
    name="rrf-full-text-search",
    type="search"
)

try:
    collection.create_search_index(model=fts_index)
    print("Search index created successfully")
except Exception as e:
    print(f"Error creating search index: {e}")