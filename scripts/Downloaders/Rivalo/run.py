import ijson
import requests
import time
import os
from datetime import date, datetime, timedelta
from requests.auth import HTTPBasicAuth
import sys

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Rivalo';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
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

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))