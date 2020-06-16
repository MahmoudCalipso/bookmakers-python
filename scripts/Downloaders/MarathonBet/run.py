import requests
import time
import os
import xml.etree.ElementTree as ET

import sys

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'MarathonBet';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Download sports feed
print('Beginning sports feed download...')
sports_feed_url = 'http://livefeeds.win/feed/scannerbet_pre?content=sports'
response = requests.get(sports_feed_url)
file = open("sports.xml", "wb")
file.write(response.text.encode('utf-8'))
file.close()

# Loop sports
root = ET.parse('sports.xml').getroot()
for type_tag in root.findall('sport'):
    name = type_tag.get('name')
    id = type_tag.get('code')
    print("Looping sport " + name + " with ID " + str(id))
    # Download tournaments feed
    print('-- Beginning events feed download...')
    events_feed_url = 'http://livefeeds.marathonbet.com/feed/scannerbet_pre?sport_codes=' + str(id);
    response = requests.get(events_feed_url)

    if response.text:
        if not os.path.exists(queue_downloader_path):
            os.makedirs(queue_downloader_path)

        file = open(queue_downloader_path + "events-" + str(id) + ".xml", "wb")
        file.write(response.text.encode('utf-8'))
        file.close()

        event_feeds.append("events-" + str(id) + ".xml")
                                

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

# Add to queue
if len(event_feeds):
    with open(queue_csv_path, 'a') as fd:
        fd.write(bookmaker_title + ';' + timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))