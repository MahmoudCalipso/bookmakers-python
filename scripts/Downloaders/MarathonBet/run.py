import requests
import time
import os
import xml.etree.ElementTree as ET
from datetime import date
import sys
import socket
import json

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'MarathonBet';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Download sports feed
print('Beginning sports feed download...')
sports_feed_url = 'http://livefeeds.win/feed/scannerbet_pre?content=sports'
with requests.get(sports_feed_url, stream=True) as r:
    r.raise_for_status()

    with open("sports.xml", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

# Loop sports
root = ET.parse('sports.xml').getroot()
for type_tag in root.findall('sport'):
    name = type_tag.get('name')
    id = type_tag.get('code')
    print("Looping sport " + name + " with ID " + str(id))
    # Download tournaments feed
    print('-- Beginning events feed download...')
    events_feed_url = 'http://livefeeds.marathonbet.com/feed/scannerbet_pre?sport_codes=' + str(id)

    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    with requests.get(events_feed_url, stream=True) as r:
        with open(queue_downloader_path + "events-" + str(id) + ".xml", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    event_feeds.append("events-" + str(id) + ".xml")
                                

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

# Add to queue
if len(event_feeds):
    with open(queue_csv_path, 'a') as fd:
        fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

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
        'feeds': event_feeds
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))