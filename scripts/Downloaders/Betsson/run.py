import ijson
import requests
import time
import os
import sys
from datetime import date

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Betsson';
download_type = 'live' if is_live else 'prematch';

current_page = 1
total_pages = 0

def download():
	global current_page
	global total_pages
	global is_live

	headers = {
	        'Content-Type': 'application/json',
	}
	feed_url = 'https://aff-api-a.bpsgameserver.com/isa/v2/601/en/event?ocb=dc2d317b-9a58-45c2-9f72-007e6a0a70b0&eventSortBy=1&eventCount=75&page=' + str(current_page)

	if is_live:
		feed_url += '&eventPhase=2'
	else:
		feed_url += '&eventPhase=1&eventStartingIn=10080'

	print(feed_url)
	if not os.path.exists(queue_downloader_path):
		os.makedirs(queue_downloader_path)

	with requests.get(feed_url, stream=True, headers=headers) as r:
		r.raise_for_status()

		with open(queue_downloader_path + "events-" + str(current_page) + ".json", 'wb') as f:
			for chunk in r.iter_content(chunk_size=8192): 
				f.write(chunk)

		event_feeds.append("events-" + str(current_page) + ".json")
		
		if total_pages == 0:
			tp = 1
			for prefix, event, value in ijson.parse(open(queue_downloader_path + "events-" + str(current_page) + ".json", 'r')):
				if prefix == 'tp':
					tp = value;
					break;
			total_pages = tp

		print('Downloading page ' + str(current_page) + ' of ' + str(total_pages))

		if current_page < total_pages:
			current_page += 1
			download()

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Download events feed
print('Beginning feed download...')
download()

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))