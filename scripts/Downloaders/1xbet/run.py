import ijson
import requests
import time
import os

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + '1xbet/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'https://part.upnp.xyz/PartLine/GetAllFeedGames?lng=en';
response = requests.get(events_feed_url)

if response.text:
    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    file = open(queue_downloader_path + "events.json", "wb")
    file.write(response.text.encode('utf-8'))
    file.close()

    event_feeds.append("events.json")

# Add to queue
with open(queue_csv_path, 'a') as fd:
    fd.write('1xbet;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))