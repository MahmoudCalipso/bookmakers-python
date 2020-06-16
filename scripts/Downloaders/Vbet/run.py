import ijson
import requests
import time
import os
import sys

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Vbet';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'https://vbetaffiliates-admin.com/global/feed/json/?language=eng&timeZone=200&brandId=4&filterData[type][]=1' if is_live else 'https://vbetaffiliates-admin.com/global/feed/json/?language=eng&timeZone=200&filterData[start_ts]=172800&brandId=4';
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
	    fd.write(bookmaker_title + ';' + timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))