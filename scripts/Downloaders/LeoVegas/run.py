import ijson
import requests
import time
import os
import sys
from datetime import date

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'LeoVegas';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Download sports feed
print('Beginning sports feed download...')
sports_feed_url = 'https://sports-offering.leovegas.com/offering/v2018/es/group.json?lang=en_US&market=all'
with requests.get(sports_feed_url, stream=True) as r:
    with open("sports.json", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

# Loop sports
sports = ijson.items(open('sports.json', 'r'), 'group.groups.item');
for sport in sports:
    name = sport.get('name')
    id = sport.get('termKey')
    print("Looping sport " + name + " with ID " + str(id))
    # Download tournaments feed
    print('-- Beginning events feed download...')
    events_feed_url = 'https://sports-offering.leovegas.com/offering/v2018/es/listView/' + str(id) + '?lang=en_US&market=all&includeParticipants=true';

    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    with requests.get(events_feed_url, stream=True) as r:
        with open(queue_downloader_path + "events-" + str(id) + ".json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    event_feeds.append("events-" + str(id) + ".json")
                                

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

# Add to queue
if len(event_feeds):
    with open(queue_csv_path, 'a') as fd:
        fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))