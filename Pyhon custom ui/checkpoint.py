import requests as req
import json
import sys,os

def imdb_api_call(requestURL,parameters):
    try:
        responce = req.get(url = requestURL, params=parameters)
        if responce.status_code != 200:
            exit()
        data = responce.json()
        return json.dumps(data)
    except:
        print("Error")

def get_upcoming_movies_by_page(api_key, page_number =1):    
    requestURL = "https://api.themoviedb.org/3/movie/upcoming"
    parameters = {"api_key": api_key, "page": page_number}
    return imdb_api_call(requestURL, parameters)

def checkpoint(checkpoint_file, movie_id):
    with open(checkpoint_file, "r") as file:
        id_list = file.read().splitlines()
        return(movie_id in id_list)
    
def write_to_checkpoint_file(checkpoint_file, movie_id):
    with open(checkpoint_file, "a") as file:
        file.writelines(movie_id +"\n")

def stream_to_splunk(checkpoint_file, data):
    for dt in data:
        if checkpoint(checkpoint_file, str(dt["id"])):
            continue
        else:
            write_to_checkpoint_file(checkpoint_file, str(dt["id"]))
            print(json.dumps(dt))

def main():
    api_key = "ac6dc639828f821502097a149dc24061"
    checkpoint_file = os.path.join(os.environ.get("SPLUNK_HOME", ""), "etc", "apps", "imdb", "bin", "checkpoint", "checkpoint.txt")
    upcoming_movie_list = get_upcoming_movies_by_page(api_key,1)
    data =json.loads(upcoming_movie_list)
    stream_to_splunk(checkpoint_file,data["results"])
    print(json.dumps(data["results"]))

if __name__=="__main__":
    main()



