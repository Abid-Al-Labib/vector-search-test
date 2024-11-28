from pymongo.mongo_client import MongoClient
import requests

model_id = "sentence-transformers/all-MiniLM-L6-v2"
hf_token = "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"


api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
headers = {"Authorization": f"Bearer {hf_token}"}

def query(texts):
    response = requests.post(api_url, headers=headers, json={"inputs": texts, "options":{"wait_for_model":True}})
    return response.json()


uri = "mongodb+srv://techabidallabib:L9QzyLDpkZYzdWDI@vectorclustertest.pxmtf.mongodb.net/?retryWrites=true&w=majority&appName=vectorClusterTest"
client = MongoClient(uri)

# Access your database and collection
database = client["sample_mflix"]
movies_collection = database["movies"]
embedded_movies_collection = database["embedded_movies"]

# plot_list_pre_embedding = []  
# # Fetch and print the first 20 documents
# for movie in collection.find().limit(5):
#     plot_list_pre_embedding.append(movie["plot"])
# print(plot_list_pre_embedding)
# embedded_plot_list = query(plot_list_pre_embedding) 
# print(embedded_plot_list)

# Batch Size
batch_size = 50  # Adjust batch size according to your needs



def process_batches():
    total_movies = movies_collection.count_documents({})  # Get the total number of movies
    print(f"Total movies to process: {total_movies}")

    for skip in range(0, 100, batch_size):
        # Fetch the batch of movie plots
        batch_movies = list(movies_collection.find().skip(skip).limit(batch_size))
        plot_list_pre_embedding = []
        movie_ids = []

        # Collect the plots and movie IDs for the current batch
        for movie in batch_movies:
            if "plot" in movie:
                plot_list_pre_embedding.append(movie["plot"])
                movie_ids.append(movie["_id"])

        print(f"Processing batch {skip // batch_size + 1}...")

        embedded_plot_list = query(plot_list_pre_embedding)

        if embedded_plot_list:
            # Create the new documents for the embedded_movies collection
            for i, movie_id in enumerate(movie_ids):
                # Update the movie document with its embedding
                print(batch_movies[i])
                embedded_movie = {
                    "_id": movie_id,
                    "plot_embedding": embedded_plot_list[i],
                    **batch_movies[i]  # Include all the original movie data
                }
                # Insert the embedded movie document into the new collection
                embedded_movies_collection.insert_one(embedded_movie)

        else:
            print(f"Failed to fetch embeddings for batch starting at movie index {skip}.")




process_batches()