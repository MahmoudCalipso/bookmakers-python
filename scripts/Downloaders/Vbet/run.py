import ijson
import requests
import time
import os
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

bookmaker_title = 'Vbet';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
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