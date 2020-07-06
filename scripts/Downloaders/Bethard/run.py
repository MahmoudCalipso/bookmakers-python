import requests
import time
import os
import gzip
import shutil
import sys
from datetime import date, datetime
import socket
import json

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Bethard';
download_type = 'live' if is_live else 'prematch';

started_at = datetime.now().strftime('%Y-%m-%d@%H:%M:%S')
start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://bethardxml.sbtech.com/lines.aspx?OddsStyle=DECIMAL&IncludeLinesIDs=true';

if is_live:
    events_feed_url += '&eventtype=39&eventtype=2560';

print(events_feed_url)
headers = {
	'Accept-Encoding': 'deflate, gzip'	
}

if not os.path.exists(queue_downloader_path):
    os.makedirs(queue_downloader_path)

with requests.get(events_feed_url, stream=True, headers=headers) as r:
    r.raise_for_status()

    with open(queue_downloader_path + "events.xml", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

event_feeds.append("events.xml")

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
        'started_at': started_at
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))