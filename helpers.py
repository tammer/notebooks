import time
import requests
import json
def wait_for_results(query_id, silent=False):
    api_header = {'X-API-Key': open('key.txt').read().strip()}
    while True:
        status_response = requests.get('https://api.streambatch.io/check?query_id={}'.format(query_id), headers=api_header)
        status = json.loads(status_response.text)
        if status['status'] == 'Succeeded':
            if not silent:
                print("\nFinal Status: Succeeded")
            return
        elif status['status'] == 'Succeeded':
            if not silent:
                print("\n Final Status: Failed!")
            return
        else:
            if not silent:
                print(f"Current Status: {status['status']} ({time.ctime(time.time())})                  ",end="\r")
            time.sleep(10)
