import time
import requests
import json
import matplotlib.pyplot as plt
import numpy as np

def wait_for_results(query_id, silent=False):
    api_header = {'X-API-Key': open('key.txt').read().strip()}
    while True:
        status_response = requests.get('https://api.streambatch.io/check?query_id={}'.format(query_id), headers=api_header)
        status = json.loads(status_response.text)
        if status['status'] == 'Succeeded':
            if not silent:
                print("\nFinal Status: Succeeded")
            return
        elif status['status'] == 'Failed':
            if not silent:
                print("\n Final Status: Failed!")
            return
        else:
            if not silent:
                print(f"Current Status: {status['status']} ({time.ctime(time.time())})                  ",end="\r")
            time.sleep(10)

def generic_plot(y,column='ndvi.sentinel2',scatter=True,s=10,title="NDVI"):
    time_array = np.array(y['time'])
    y_array = np.array(y[column])
    plt.figure(figsize=(12, 3))
    if scatter:
        plt.scatter(time_array, y_array,s=s)
    else:
        plt.plot(time_array, y_array)
    plt.xlabel('Time')
    plt.ylabel(column)
    # make y axis start at 0 and end at 1.0
    plt.ylim(0, 1.0)
    # add a title to the plot
    plt.title(title)
    plt.show()
