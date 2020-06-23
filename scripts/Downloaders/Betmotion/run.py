import requests
import time
import os
import sys
from datetime import date

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Betmotion';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://dataexport-uof-betmotion.biahosted.com/Export/GetLiveEvents?importerId=2919' if is_live else 'http://dataexport-uof-betmotion.biahosted.com/Export/GetEvents?importerId=2919';
print(events_feed_url)

if not os.path.exists(queue_downloader_path):
    os.makedirs(queue_downloader_path)

with requests.get(events_feed_url, stream=True) as r:
    r.raise_for_status()

    with open(queue_downloader_path + "events.json", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

event_feeds.append("events.json")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))