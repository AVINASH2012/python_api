
import requests as req
import json
import sys
import os

def imdb_api_call(request_url, parameters):
    try:
        response = req.get(url=request_url, params=parameters)
        if response.status_code != 200:
            exit()
        data = response.json()
        return json.dumps(data)
    except:
        print("Error")

def get_upcoming_movies_by_page(api_key, page_number=1, criteria=None):
    request_url = "https://api.themoviedb.org/3/movie/upcoming"
    parameters = {"api_key": api_key, "page": page_number}

    if criteria:
        parameters.update(criteria)

    return imdb_api_call(request_url, parameters)

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
            movie_info = [str(dt["id"]), str(dt["vote_average"]),str(dt["title"]), str(dt["release_date"])]
            write_to_checkpoint_file(checkpoint_file, movie_info)

            # Writing headers (if not written yet)
            if not header_written:
                write_to_checkpoint_file(checkpoint_file, ["ID", "Vote Average", "Title", "Release Date"])
                header_written = True

def main():
    api_key = "ac6dc639828f821502097a149dc24061"
    checkpoint_file = os.path.join(os.environ.get("SPLUNK_HOME", ""), "etc", "apps", "imdb", "bin", "checkpoint", "checkpoint.txt")

    # Example: Fetch the first page of upcoming movies released after a certain date
    criteria = {"release_date.gte": "2023-01-01"}
    upcoming_movie_list = get_upcoming_movies_by_page(api_key, 1, criteria)
    
    # Example: Fetch the second page of upcoming movies with a minimum vote count
    criteria = {"vote_count.gte": 500}
    upcoming_movie_list_page2 = get_upcoming_movies_by_page(api_key, 2, criteria)

    data_page1 = json.loads(upcoming_movie_list)["results"]
    data_page2 = json.loads(upcoming_movie_list_page2)["results"]

    stream_to_splunk(checkpoint_file, data_page1)
    stream_to_splunk(checkpoint_file, data_page2)

if __name__ == "__main__":
    main()
