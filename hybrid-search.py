
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

search_query = "Christmas Story"
embedded_query = query(search_query)

vectorWeight = 1
fullTextWeight = 1

pipeline = [
            {
                "$vectorSearch": {
                  "index": "vector_plot_index",
                  "path": "plot_embedding",
                  "queryVector": embedded_query,
                  "numCandidates": 100,
                  "limit": 20
                }
            }, {
                "$group": {
                  "_id": None,
                  "docs": {"$push": "$$ROOT"}
                }
            }, {
                "$unwind": {
                  "path": "$docs", 
                  "includeArrayIndex": "rank"
                }
            }, {
                "$addFields": {
                  "vs_score": {
                    "$multiply": [
                      vectorWeight, {
                        "$divide": [
                          1.0, {
                            "$add": ["$rank", 60]
                          }
                        ]
                      }
                    ]
                  }
                }
            }, {
                "$project": {
                  "vs_score": 1, 
                  "_id": "$docs._id", 
                  "title": "$docs.title"
                }
            }, {
                "$unionWith": {
                  "coll": "movies",
                  "pipeline": [
                    {
                      "$search": {
                        "index": "rrf-full-text-search",
                        "phrase": {
                          "query": search_query,
                          "path": "title"
                        }
                      }
                    }, {
                      "$limit": 20
                    }, {
                      "$group": {
                        "_id": None,
                        "docs": {"$push": "$$ROOT"}
                      }
                    }, {
                      "$unwind": {
                        "path": "$docs", 
                        "includeArrayIndex": "rank"
                      }
                    }, {
                      "$addFields": {
                        "fts_score": {
                          "$multiply": [
                            fullTextWeight, {
                              "$divide": [
                                1.0, {
                                  "$add": ["$rank", 60]
                                }
                              ]
                            }
                          ]
                        }
                      } 
                    },
                    {
                      "$project": {
                        "fts_score": 1,
                        "_id": "$docs._id",
                        "title": "$docs.title"
                      }
                    }
                  ]
                }
            }, {
                "$group": {
                  "_id": "$title",
                  "vs_score": {"$max": "$vs_score"},
                  "fts_score": {"$max": "$fts_score"}
                }
            }, {
                "$project": {
                  "_id": 1,
                  "title": 1,
                  "vs_score": {"$ifNull": ["$vs_score", 0]},
                  "fts_score": {"$ifNull": ["$fts_score", 0]}
                }
            }, {
                "$project": {
                  "score": {"$add": ["$fts_score", "$vs_score"]},
                  "_id": 1,
                  "title": 1,
                  "vs_score": 1,
                  "fts_score": 1
                }
            },
            {"$sort": {"score": -1}},
            {"$limit": 10}
        ]

result = client["sample_mflix"]["embedded_movies"].aggregate(pipeline)

for i in result:
  print(i)

