import requests
import json
import pandas as pd
import time
import pyarrow.parquet # for reading parquet files (implicit dependency)
import fsspec # for reading parquet files (implicit dependency)
import s3fs # for reading parquet files (implicit dependency)

DEBUG_NO_CALL = False # set to True to skip the API call and use exisint data on server

class StreambatchData:
    def __init__(self, api_key, space, time=None,asyncronous=False):

        # if time is not given, set it to be the full history
        if time is None:
            time = time = {'start':'2015-01-01','end':'2053-08-01','unit':'day'} # !!!
        self.time = time

        # if space is not in a list, put it in a list
        if type(space[0]) is not list:
            space = [space]
        self.space = space
        
        # set the class variables
        self.api_header = {'X-API-Key': api_key}
        self.ndvi_data = None     # the data that is returned from the server
        self.result = None        # the result of the request. Will be None or the error message if we failed
        self.final_status = None  # starts as None and will become either 'Succeeded' or 'Failed'

        if DEBUG_NO_CALL:
            print("debug no call to server")
            self.query_id = '7877840e-6a54-4ab6-b5a9-a589dee03593'
            self.access_url = f's3://streambatch-data/{self.query_id}.parquet'
        else:
            ndvi_request = {'variable': ['ndvi.sentinel2'], 'space': space, 'time': time }
            print(f"Request: {ndvi_request}")
            response = requests.post('https://api.streambatch.io/async', json=ndvi_request, headers=self.api_header)
            if response.status_code != 200:
                print("Error: request failed")
                print(f"Response: {response.status_code} {response.reason}")
                print(response.content)
                return
            print("Processing request...")
            # print(f"Response: {response.status_code} {response.reason}")
            # print(response.content)
            self.query_id = json.loads(response.content)['id']
            self.access_url = json.loads(response.content)['access_url']
        
        if asyncronous is False:
            print("Waiting for results...",end="")
            self.wait_for_results()

    def as_dataframe(self):
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
            print('.',end="")

api_key = open('key.txt').read().strip()
space = [3.940705,49.345238]
ndvi_data = StreambatchData(api_key, space)
print(ndvi_data.as_dataframe())

# asynchonous request
# ndvi_data = StreambatchData(api_key, space, asyncronous=True)
# while ndvi_data.ready() is False:
#     print(f"Processing ({time.ctime(time.time())})                  ",end="\r")
#     time.sleep(10)
# print("Results are ready!")
# print(ndvi_data.as_dataframe())

