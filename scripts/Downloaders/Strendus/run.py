import ijson
import requests
import time
import os
import sys
from datetime import date
import socket
import json

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Strendus';
download_type = 'live' if is_live else 'prematch';

current_page = 0
records_per_page = 500

def download(id):
    global current_page
    global pages
    global records_per_page

    print('Downloading page ' + str(current_page + 1))

    offset = current_page * records_per_page;
    feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetPagedMatches?Culture=en&affi=49&DateFilterType=0&WidgetType=2&StartRecord=' + str(offset) + '&EndRecord=' + str(offset + records_per_page) + '&SportID=' + str(id)
    print(feed_url)
    response = requests.get(feed_url)
    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    with requests.get(feed_url, stream=True) as r:
        with open(queue_downloader_path + "events-" + str(id) + "-" + str(current_page) + ".json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    event_feeds.append("events-" + str(id) + "-" + str(current_page) + ".json")

    items = ijson.items(open(queue_downloader_path + "events-" + str(id) + "-" + str(current_page) + ".json", 'r'), 'd.m.item');

    print(items)
    has_items = False;
    try:
        for item in items:
            has_items = True;
            break;
    except:
        has_items = False

    if has_items:
        current_page += 1
        download(id)

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

if is_live:
    events_feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetLiveMatchesMetaData?LanguageID=en&affi=49&IncludeOdds=true'
    print(events_feed_url)

    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    with requests.get(events_feed_url, stream=True) as r:
        with open(queue_downloader_path + "events.json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    event_feeds.append("events.json")
else:
    # Download sports feed
    print('Beginning sports feed download...')
    sports_feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetFixturesMenu?Culture=en&affid=49&LoadPeriod=0'
    with requests.get(sports_feed_url, stream=True) as r:
        with open("sports.json", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    # Loop sports
    sports = ijson.items(open('sports.json', 'r'), 's.item');
    for sport in sports:
        name = sport.get('n')
        id = sport.get('id')
        current_page = 0
        print("Looping sport " + name + " with ID " + str(id))
        # Download events feed
        print('Beginning events feed download...')
        download(id)

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

#if os.path.exists("tournaments.json"):
#  os.remove("tournaments.json")

# Add to queue
if len(event_feeds) > 0:
    with open(queue_csv_path, 'a') as fd:
        fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

# local host IP '127.0.0.1' 
host = '127.0.0.1'

# Define the port on which you want to connect 
port = 12345

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

# connect to server on local computer 
s.connect((host,port)) 

# message you send to server 
message = json.dumps({
    'message': 'download_complete',
    'data': {
        'bookmaker_title': bookmaker_title,
        'timestamp': timestamp,
        'sport': 'All',
        'type': download_type,
        'feeds': event_feeds
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))