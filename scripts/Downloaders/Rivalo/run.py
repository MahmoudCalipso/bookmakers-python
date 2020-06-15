import ijson
import requests
import time
import os
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + 'Rivalo/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
now = datetime.today() - timedelta(hours=1, minutes=0)
events_feed_url = 'https://rinfo-feed.rivalo.com/ebet-export/list?updatedFrom=' + now.strftime('%Y-%m-%d') + 'T' + now.strftime('%H:%M:%S');
print(events_feed_url)
response = requests.get(events_feed_url, auth=HTTPBasicAuth('scannerbet1-xml', 'kSrJoBPaEm9L'), verify=False)

if response.text:
	xmls = response.text.split("\n")
	index = 0

	if len(xmls) > 0:
		for xml in xmls:
			print('Downloading ' + xml)
			response = requests.get(xml, auth=HTTPBasicAuth('scannerbet1-xml', 'kSrJoBPaEm9L'), verify=False)

			if not os.path.exists(queue_downloader_path):
				os.makedirs(queue_downloader_path)

			file = open(queue_downloader_path + "events" + str(index) + ".xml", "wb")
			file.write(response.text.encode('utf-8'))
			file.close()

			event_feeds.append("events-" + str(index) + ".xml")
			index += 1

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write('Rivalo;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))