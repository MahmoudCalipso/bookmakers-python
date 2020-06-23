import requests
import time
import os
import gzip
import shutil
import sys
from datetime import date

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Bethard';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://bethardxml.sbtech.com/lines.aspx?OddsStyle=DECIMAL&IncludeLinesIDs=true&BranchID=1,2,14,64,11,35,6,20,43,16,12,3';

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

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))