import ijson
import requests
import time
import os
from datetime import date

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue_' + date.today().strftime("%d-%m-%Y") + '.csv';
queue_downloader_path = queue_path + 'LeoVegas/' + timestamp + '/';
event_feeds = []

# Download sports feed
print('Beginning sports feed download...')
sports_feed_url = 'https://sports-offering.leovegas.com/offering/v2018/es/group.json?lang=en_US&market=all'
response = requests.get(sports_feed_url)
file = open("sports.json", "wb")
file.write(response.text.encode('utf-8'))
file.close()

# Loop sports
sports = ijson.items(open('sports.json', 'r'), 'group.groups.item');
for sport in sports:
        name = sport.get('name')
        id = sport.get('termKey')
        print("Looping sport " + name + " with ID " + str(id))
        # Download tournaments feed
        print('-- Beginning events feed download...')
        events_feed_url = 'https://sports-offering.leovegas.com/offering/v2018/es/listView/' + str(id) + '?lang=en_US&market=all&includeParticipants=true';
        response = requests.get(events_feed_url)

        if response.text:
            if not os.path.exists(queue_downloader_path):
                os.makedirs(queue_downloader_path)

            file = open(queue_downloader_path + "events-" + str(id) + ".json", "wb")
            file.write(response.text.encode('utf-8'))
            file.close()

            event_feeds.append("events-" + str(id) + ".json")
                                

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

# Add to queue
if len(event_feeds):
    with open(queue_csv_path, 'a') as fd:
        fd.write('LeoVegas;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))