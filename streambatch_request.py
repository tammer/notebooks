import requests
import json
import pandas as pd
import time
import pyarrow.parquet # for reading parquet files (implicit dependency)
import fsspec # for reading parquet files (implicit dependency)
import s3fs # for reading parquet files (implicit dependency)

class StreambatchRequest:
    def __init__(self, api_key, space, time=None,asyncronous=False,silent=False,debug=False):

        # set the class variables
        self.api_header = {'X-API-Key': api_key}
        self.ndvi_data = None     # the data that is returned from the server
        self.result = None        # the result of the request. Will be None or the error message if we failed
        self.final_status = None  # starts as None and will become either 'Succeeded' or 'Failed'
        self.silent = silent      # if True, don't print anything
        
        if type(space) is dict:
            # it must by a single polygon, put it in a list
            space = [space]
        elif type(space) is list:
            if len(space) == 0:
                print("Request Failed: space parameter is an empty list.")
                return
            # ok, it is a list, but what is item 0?
            if type(space[0]) is dict:
                # great, list of polygons
                None
            elif type(space[0]) is list:
                # great, list of [lat,long]s
                None
            else:
                # must be a single [lat,long]
                space = [space]
        else:
            print("Request Failed: space parameter is in a format that is not understood.")
            return
        
        self.space = space

        if not silent: print("Number of locations: {}".format(len(space)))

        # if time is not given, set it to be the full history
        if time is None:
            time = time = { 'start':'2014-01-01',
                            'end':self.today(),
                            'unit':'day'}
        self.time = time

        if not silent: print("Range: {} - {}, unit: {}".format(time['start'],time['end'],time['unit']))

        if debug:
            print("debug no call to server")
            self.query_id = '8fae9b4c-7740-4c3f-af80-8fb558ff5a7a'
            self.access_url = f's3://streambatch-data/{self.query_id}.parquet'
        else:
            ndvi_request = {'variable': ['ndvi.sentinel2'], 'space': space, 'time': time }
            response = requests.post('https://api.streambatch.io/async', json=ndvi_request, headers=self.api_header)
            if response.status_code != 200:
                print("Error: request failed")
                print(f"Response: {response.status_code} {response.reason}")
                print(response.content)
                return
            self.query_id = json.loads(response.content)['id']
            self.access_url = json.loads(response.content)['access_url']
            if not silent: print("Query ID: {}".format(self.query_id))
        
        if asyncronous is False:
            if not silent: print("Waiting for results...",end="",flush=True)
            self.wait_for_results()

    def get_data(self):
        if self.ndvi_data is None and self.final_status == 'Succeeded':
            self.ndvi_data = pd.read_parquet(self.access_url, storage_options={"anon": True})
        return self.ndvi_data

    def status(self):
        if self.final_status is not None:
            return self.status
        status_response = requests.get('https://api.streambatch.io/check?query_id={}'.format(self.query_id), headers=self.api_header)
        status = json.loads(status_response.text)
        if status['status'] == 'Succeeded':
            self.final_status = 'Succeeded'
        elif status['status'] == 'Failed':
            self.final_status = 'Failed'
            self.result = status
        return status['status']

    def ready(self):
        if self.final_status is not None:
            return True
        else:
            self.status()
            return self.final_status is not None

    def wait_for_results(self):
        while self.ready() is False:
            self.status() # causes self.ready to be updated
            time.sleep(7)
            if not self.silent: print('.',end="",flush=True)
        if not self.silent: print("")
    
    def today(self): return time.strftime("%Y-%m-%d")

    

# api_key = open('key.txt').read().strip()
# space = [3.940705,49.345238]
# ndvi_data = StreambatchData(api_key, space,debug=False)
# print(ndvi_data.as_dataframe())

# asynchonous request
# ndvi_data = StreambatchData(api_key, space, asyncronous=True)
# while ndvi_data.ready() is False:
#     print(f"Processing ({time.ctime(time.time())})                  ",end="\r")
#     time.sleep(10)
# print("Results are ready!")
# print(ndvi_data.as_dataframe())

