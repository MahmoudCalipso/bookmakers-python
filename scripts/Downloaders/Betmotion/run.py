import requests
import time
import os
import gzip
import shutil

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + 'Betmotion/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://dataexport-uof-betmotion.biahosted.com/Export/GetEvents?importerId=2919';
response = requests.get(events_feed_url)

if response.text:
    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    file = open(queue_downloader_path + "events.json", "wb")
    file.write(response.text.encode('utf-8'))
    file.close()
    event_feeds.append("events.json")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write('Betmotion;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))