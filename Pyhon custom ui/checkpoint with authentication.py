import requests
import json
import sys
import os

class MovieAPIManager:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url

    def authenticate(self):
        # Assume authentication is not required for this example
        return self.api_key

    def api_call(self, endpoint, parameters=None, headers=None):
        full_url = f"{self.base_url}{endpoint}"

        if headers is None:
            headers = {}

        headers.update({"Authorization": f"Bearer {self.authenticate()}"})
        
        try:
            response = requests.get(url=full_url, params=parameters, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API call error: {e}")
            sys.exit(1)

def checkpoint(checkpoint_file, movie_id):
    with open(checkpoint_file, "r") as file:
        id_list = file.read().splitlines()
        return movie_id in id_list

def write_to_checkpoint_file(checkpoint_file, movie_info):
    with open(checkpoint_file, "a") as file:
        file.writelines("\t".join(map(str, movie_info)) + "\n")

def stream_to_splunk(checkpoint_file, data):
    header_written = False  # To track whether the header has been written

    for dt in data:
        if checkpoint(checkpoint_file, str(dt["id"])):
            continue
        else:
            # Extracting only ID, version, name, and date fields
            movie_info = [dt["id"], dt["vote_average"], dt["title"], dt["release_date"], dt["overview"]]
            write_to_checkpoint_file(checkpoint_file, movie_info)

            # Writing headers (if not written yet)
            if not header_written:
                write_to_checkpoint_file(checkpoint_file, ["ID", "Vote Average", "Title", "Release Date", "Overview"])
                header_written = True

def main():
    api_key1 = "ac6dc639828f821502097a149dc24061"  # Replace with your API key 1
    base_url1 = "https://api.themoviedb.org/3/movie/upcoming"  # Replace with the base URL of API 1

    api_manager1 = MovieAPIManager(api_key1, base_url1)

    checkpoint_file = os.path.join(os.environ.get("SPLUNK_HOME", ""), "etc", "apps", "imdb", "bin", "checkpoint", "checkpoint.txt")

    # Example: Fetch the first page of upcoming movies from API 1 released after a certain date
    criteria1 = {"release_date.gte": "2023-01-01"}
    data_page1_api1 = api_manager1.api_call("", parameters=criteria1)["results"]

    # Process and stream data as needed
    stream_to_splunk(checkpoint_file, data_page1_api1)

if __name__ == "__main__":
    main()
