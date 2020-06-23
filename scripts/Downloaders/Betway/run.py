import requests
import time
import os
import sys
from datetime import date

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Betway';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'https://feeds.betway.com/sbeventsliveen' if is_live else 'https://feeds.betway.com/sbeventsen';
events_feed_url += '?key=83594BE8&keywords=soccer,basketball,motor-sport,motor-racing,formula-1,esports,rugby-league,rugby-union,tennis,boxing,ufc---martial-arts,cycling,golf,american-football'

with requests.get(events_feed_url, stream=True) as r:
	r.raise_for_status()
	if not os.path.exists(queue_downloader_path):
		os.makedirs(queue_downloader_path)

	with open(queue_downloader_path + "events.xml", 'wb') as f:
		for chunk in r.iter_content(chunk_size=8192): 
			# If you have chunk encoded response uncomment if
			# and set chunk_size parameter to None.
			#if chunk: 
			f.write(chunk)

event_feeds.append("events.xml")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))