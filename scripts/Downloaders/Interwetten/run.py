import ijson
import requests
import time
import os
import json
import sys
from datetime import date
import socketio

sio = socketio.Client()

@sio.event
def connect():
    print('Connection established')

@sio.event
def connect_error():
    print("The connection failed!")

@sio.event
def disconnect():
    print('Disconnected from server')

sio.connect('http://127.0.0.1:5000', namespaces=['/readers'])

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Interwetten';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Auth
print('Retrieving auth token...')
headers = {
    'Content-Type': 'application/json',
}
params = {
	'username': 'scannerbet_7', 
	'password': 'MBJ8MhS&rQ47%F$CUNU76BzME'
}
auth_url = 'https://partner.odds.ws/api/Auth/Login'
response = requests.post(auth_url, json.dumps(params), headers=headers)
file = open("auth.json", "wb")
file.write(response.text.encode('utf-8'))
file.close()

# Get token
token = None
for prefix, event, value in ijson.parse(open('auth.json', 'r')):
	if prefix == 'token':
		token = value;
		break;

if token:
	# Download sports feed
	print('Beginning sports feed download...')
	headers = {
		'Authorization': 'Bearer ' + token,
	}
	sports_feed_url = 'https://partner.odds.ws/api/Sports/EN'
	with requests.get(sports_feed_url, stream=True, headers=headers) as r:
	    r.raise_for_status()

	    with open("sports.json", 'wb') as f:
	        for chunk in r.iter_content(chunk_size=8192): 
	            f.write(chunk)

	# Loop sports
	sports = ijson.items(open('sports.json', 'r'), 'items.item');
	for sport in sports:
		name = sport.get('name')
		id = sport.get('id')
		print("Looping sport " + name + " with ID " + str(id))
		print('-- Beginning events feed download...')

		events_feed_url = 'https://partner.odds.ws/api/sport/EN/' + str(id) + '?eventFilter=' + ('Live' if is_live else 'PreMatch') + '&marketFilter=All'
		print(events_feed_url)
		if response.text:
			if not os.path.exists(queue_downloader_path):
			    os.makedirs(queue_downloader_path)

		with requests.get(events_feed_url, stream=True, headers=headers) as r:
		    with open(queue_downloader_path + "events-" + str(id) + ".json", 'wb') as f:
		        for chunk in r.iter_content(chunk_size=8192): 
		            f.write(chunk)

		event_feeds.append("events-" + str(id) + ".json")

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

#if os.path.exists("tournaments.json"):
#  os.remove("tournaments.json")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

sio.emit('download_complete', {
    'bookmaker': bookmaker_title,
    'timestamp': timestamp,
    'sport': 'All',
    'type': download_type,
    'feeds': event_feeds
}, namespace='/readers')

sio.sleep(5)
print('Disconnecting!')
sio.disconnect()

print("--- %s seconds ---" % (time.time() - start_time))