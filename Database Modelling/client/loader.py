import json
import argparse
import sys
import requests
from requests.exceptions import ConnectionError, ConnectTimeout

load_url="songs"


## takes in raw data of songs with artists

#load data

#run a few endpoints on it
#check statuses
#check return contents

def run_loader(config):
    #recreate tables
    ALL=False
    batch_folder = None
    if config.file:
        if config.all:
            ALL = True
    
    local_host = "http://127.0.0.1:"
    port = "5000"
    if config.port:
        port = str(config.port)
    app_url = local_host + port + "/"
    if config.create or ALL:
        create_url = app_url + "create"
        r = requests.get(create_url)
        if r.status_code != 200: 
            print("Error Creating.  %s  Body: %s" % (r,r.content))
        else:
            print("Created")
    
    #load data in
    if config.file:
        with open(config.file) as jfile:
            json_input = json.load(jfile)
            post_url = app_url + json_input["path"]
            json_input = json_input["data"]

            print("Using post url to load %s" % post_url)
            if config.single:
                load_single(config, json_input, post_url)         
            else:
                for x in json_input:
                    load_single(config, x, post_url)

    #run single test
    if config.test:
        test_file(config.test, app_url)
    
    batch_folder = None

    #run batch tests
    if config.batch:
        batch_folder = config.batch

    #grabbing presumed test batch folder name
    if ALL:
        batch_folder = config.file.replace(".json", "").replace("../data/", "")   

    if batch_folder:   
        #run tests in endpoints
        endpoints = "../tests/simple_endpoints.json"
        if config.endpoints:
            endpoints = config.endpoints

        #run tests for each endpoint
        with open(endpoints) as jfile:
            json_endpoints = json.load(jfile)
            for e in json_endpoints:
                print(e["test_name"])
                test_file("../tests/"+ batch_folder + "/" + str(e["test_name"]), app_url)
    

def test_file(file, app_url):
    print(str(file))
    with open(file) as jfile:
            json_test = json.load(jfile)
            #fetches MS path for test
            get_url = app_url + json_test["get_path"]
            print("Using post url to get %s" % get_url)
            if config.single:
                get_single(config, json_test["tests"], get_url)         
            else:
                if "tests" in json_test:
                    for x in json_test["tests"]:
                        get_single(config, x, get_url)
                else:
                    get_single(config, json_test["output"], get_url)


def load_single(config, json_input, post_url):
    try:
        r = requests.post(post_url,json=json_input)
        if r.status_code >= 400:
            print("Error.  %s  Body: %s" % (r,r.content))
        else: 
            print("Resp: %s  Body: %s" % (r,r.content))
            #check that response is 201, added new song
            if (r.status_code != 201):
                print("Expect response 201, got:", r.status_code)
                
            #check that body is as expected (dict id), we can 
            #just check in getter

    except ConnectionError as err:
        print("Connection error, halting %s" % err)
        return
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

def get_single(config, json_test, get_url):
    try:
        if "inputs" not in json_test:
            #if its just one endpoint (e.g. top_length)
            expected = json_test
        else:
            input = json_test["inputs"]
            #appending parameters into get_url 
            expected = json_test["expected"]
            get_url += '/' + str(input)
        
        #grab the response
        r = requests.get(get_url)
        if r.status_code >= 400:
            print("Error at %s.  %s  Body: %s" % (get_url, r,r.content))
        else: 
            print("Resp: %s" % (r))
            #print(json_test["expected"])
            if expected != r.json():
                print("===unexpected return at" + get_url + "===")
                print("expected json: %s" % expected)
                print("actual: %s" % r.json())
                print("=======================")
    except ConnectionError as err:
        print("Connection error, halting %s" % err)
        return
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-A","--all", dest="all", help="Runs through everything for file",action="store_true")
    parser.add_argument("-f","--file", dest="file", help="Input json file to load")
    parser.add_argument("-c", "--create", dest="create", help="Recreate tables",action="store_true")
    parser.add_argument("-t", "--test", dest="test", help="Single test json file")
    parser.add_argument("-b", "--batch", dest="batch", help="Batch tests Folder")
    parser.add_argument("-e", "--endpoints", dest="endpoints", help="Run all test for endpoints")
    parser.add_argument("-p", "--port", dest="port", help="Port Flask App is running on")
    parser.add_argument("--single", help="Call a loader for a JSON file with a single entry",action="store_true")
    config = parser.parse_args()
    run_loader(config)