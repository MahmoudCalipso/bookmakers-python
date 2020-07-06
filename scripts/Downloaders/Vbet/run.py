import ijson
import requests
import time
import os
import sys
from datetime import date, datetime
import socket
import json

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Vbet';
download_type = 'live' if is_live else 'prematch';

started_at = datetime.now().strftime('%Y-%m-%d@%H:%M:%S')
start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'https://vbetaffiliates-admin.com/global/feed/json/?language=eng&timeZone=200&brandId=4&filterData[type][]=1' if is_live else 'https://vbetaffiliates-admin.com/global/feed/json/?language=eng&timeZone=200&filterData[start_ts]=172800&brandId=4';

if not os.path.exists(queue_downloader_path):
    os.makedirs(queue_downloader_path)

path = queue_downloader_path + "events.json"

with open(path, 'a') as file:
	file.write("{\"items\": [")

with requests.get(events_feed_url, stream=True) as r:
    with open(path, 'ab') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

with open(path, 'a') as file:
	file.write("]}")

event_feeds.append("events.json")

# local host IP '127.0.0.1' 
host = '127.0.0.1'

# Define the port on which you want to connect 
port = 12345

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

# connect to server on local computer 
s.connect((host,port)) 

# message you send to server 
message = json.dumps({
    'message': 'download_complete',
    'data': {
        'bookmaker_title': bookmaker_title,
        'timestamp': timestamp,
        'sport': 'All',
        'type': download_type,
        'feeds': event_feeds,
        'started_at': started_at
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))