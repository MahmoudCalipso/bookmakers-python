import ijson
import requests
import time
import os
from datetime import date, datetime, timedelta
from requests.auth import HTTPBasicAuth
import sys
import socket
import json

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Rivalo';
download_type = 'live' if is_live else 'prematch';

started_at = datetime.now().strftime('%Y-%m-%d@%H:%M:%S')
start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
now = datetime.today() - timedelta(hours=1, minutes=0)
events_feed_url = 'https://rinfo-feed.rivalo.com/ebet-export/liveEvents?language=en' if is_live else 'https://rinfo-feed.rivalo.com/ebet-export/list'

if not is_live:
	events_feed_url += '?updatedFrom=' + now.strftime('%Y-%m-%d') + 'T' + now.strftime('%H:%M:%S');

print(events_feed_url)
response = requests.get(events_feed_url, auth=HTTPBasicAuth('scannerbet1-xml', 'kSrJoBPaEm9L'), verify=False)

if response.text:
	xmls = response.text.split("\n")
	index = 0

	if len(xmls) > 0:
		for xml in xmls:
			print('Downloading ' + xml)
			if len(xml) > 0:
				if not os.path.exists(queue_downloader_path):
					os.makedirs(queue_downloader_path)

				with requests.get(xml, auth=HTTPBasicAuth('scannerbet1-xml', 'kSrJoBPaEm9L'), verify=False, stream=True) as r:
					with open(queue_downloader_path + "events-" + str(index) + ".xml", 'wb') as f:
						for chunk in r.iter_content(chunk_size=8192): 
							f.write(chunk)

				event_feeds.append("events-" + str(index) + ".xml")
				index += 1

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