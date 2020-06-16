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
queue_csv_path = queue_path + 'queue_' + date.today().strftime("%d-%m-%Y") + '.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://feeds.betway.com/sbeventsliveen' if is_live else 'http://feeds.betway.com/sbeventsen';
events_feed_url += '?key=83594BE8&keywords=soccer,basketball,motor-sport,motor-racing,formula-1,esports,rugby-league,rugby-union,tennis,boxing,ufc---martial-arts,cycling,golf,american-football'

print(events_feed_url)
response = requests.get(events_feed_url)

if response.text:
	if not os.path.exists(queue_downloader_path):
		os.makedirs(queue_downloader_path)

	file = open(queue_downloader_path + "events.json", "wb")
	file.write(response.text.encode('utf-8'))
	file.close()
	event_feeds.append("events.xml")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(bookmaker_title + ';' + timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))