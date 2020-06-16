import ijson
import requests
import time
import os
import sys
from datetime import date

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

    if response.text:
        if not os.path.exists(queue_downloader_path):
            os.makedirs(queue_downloader_path)

        file = open(queue_downloader_path + "events-" + str(id) + "-" + str(current_page) + ".json", "wb")
        file.write(response.text.encode('utf-8'))
        file.close()
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
queue_csv_path = queue_path + 'queue_' + date.today().strftime("%d-%m-%Y") + '.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

if is_live:
    events_feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetLiveMatchesMetaData?LanguageID=en&affi=49&IncludeOdds=true'
    print(events_feed_url)
    response = requests.get(events_feed_url)

    if response.text:
        if not os.path.exists(queue_downloader_path):
            os.makedirs(queue_downloader_path)

    file = open(queue_downloader_path + "events.json", "wb")
    file.write(response.text.encode('utf-8'))
    file.close()

    event_feeds.append("events-.json")
else:
    # Download sports feed
    print('Beginning sports feed download...')
    sports_feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetFixturesMenu?Culture=en&affid=49&LoadPeriod=0'
    response = requests.get(sports_feed_url)
    file = open("sports.json", "wb")
    file.write(response.text.encode('utf-8'))
    file.close()

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
        fd.write(bookmaker_title + ';' + timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))