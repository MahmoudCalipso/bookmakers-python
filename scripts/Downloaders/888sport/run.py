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

bookmaker_title = '888sport';
download_type = 'live' if is_live else 'prematch';

started_at = datetime.now().strftime('%Y-%m-%d@%H:%M:%S')
start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

if is_live:
    print('-- Beginning events feed download...')
    events_feed_url = 'http://www.smart-feeds.com/getfeeds.aspx?Param=event/live/open';
    print(events_feed_url)

    with requests.get(events_feed_url, stream=True) as r:
        r.raise_for_status()
        if not os.path.exists(queue_downloader_path):
            os.makedirs(queue_downloader_path)

        with open(queue_downloader_path + "events.json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
else:
    # Download sports feed
    print('Beginning sports feed download...')
    sports_feed_url = 'http://www.smart-feeds.com/getfeeds.aspx?Param=group'
    with requests.get(sports_feed_url, stream=True) as r:
        r.raise_for_status()

        with open("sports.json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    # Loop sports
    sports = ijson.items(open('sports.json', 'r'), 'group.groups.item');
    for sport in sports:
        name = sport.get('englishName')
        id = sport.get('id')
        print("Looping sport " + name + " with ID " + str(id))
        # Download tournaments feed
        #print('-- Beginning events feed download...')
        events_feed_url = 'http://www.smart-feeds.com/getfeeds.aspx?Param=event/group/' + str(id) + '&includeParticipants=true';

        if not os.path.exists(queue_downloader_path):
            os.makedirs(queue_downloader_path)

        with requests.get(events_feed_url, stream=True) as r:
            r.raise_for_status()

            with open(queue_downloader_path + "events-" + str(id) + ".json", 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

        event_feeds.append("events-" + str(id) + ".json")

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