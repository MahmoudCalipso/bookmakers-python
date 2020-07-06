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

bookmaker_title = '1xbet';
download_type = 'live' if is_live else 'prematch';

started_at = datetime.now().strftime('%Y-%m-%d@%H:%M:%S')
start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'https://part.upnp.xyz/PartLive/GetAllFeedGames?lng=en' if is_live else 'https://part.upnp.xyz/PartLine/GetAllFeedGames?lng=en';
print(events_feed_url)

with requests.get(events_feed_url, stream=True) as r:
	if not os.path.exists(queue_downloader_path):
		os.makedirs(queue_downloader_path)

	with open(queue_downloader_path + "events.json", 'wb') as f:
		for chunk in r.iter_content(chunk_size=8192): 
			# If you have chunk encoded response uncomment if
			# and set chunk_size parameter to None.
			#if chunk: 
			f.write(chunk)

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
	    'started_at': started_at
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))