import ijson
import requests
import time
import os
import sys
from datetime import date
import socketio

sio = socketio.Client()

@sio.event
def connect():
    print('Connection established')

@sio.event
def connect_error():
    print("The connection failed!")

@sio.event
def disconnect():
    print('Disconnected from server')

sio.connect('http://127.0.0.1:5000', namespaces=['/readers'])

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Codere';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Download sports feed
print('Beginning sports feed download...')
headers = {
        'CodereAffiliateApiKey': 'idbmob_API',
        'CodereAffiliateApiSecret': '8d2577601b4c38200522322c69d3c22a'
}
sports_feed_url = 'http://coderesbgonlinesbs.azurewebsites.net/api/feeds/sports'

with requests.get(sports_feed_url, stream=True, headers=headers) as r:
    r.raise_for_status()

    with open("sports.json", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)

# Loop sports
sports = ijson.items(open('sports.json', 'r'), 'item');
for sport in sports:
    name = sport.get('Name')
    id = sport.get('NodeId')
    print("Looping sport " + name + " with ID " + id)
    # Download tournaments feed
    print('-- Beginning tournaments feed download...')
    leagues_feed_url = 'http://coderesbgonlinesbs.azurewebsites.net/api/feeds/sports/' + id + '/leagues'

    try:

        with requests.get(leagues_feed_url, stream=True, headers=headers) as r:
            r.raise_for_status()

            with open("tournaments.json", 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)


        # Loop tournaments and get events feed
        tournaments = ijson.items(open('tournaments.json', 'r'), 'item')
        for tournament in tournaments:
            leagues = tournament.get('Leagues')
            for league in leagues:
                # Download events feed
                name = league.get('Name')
                id = league.get('NodeId')
                print("---- Looping tournament " + name + " with ID " + id)
                print('------ Beginning events feed download...')

                if is_live:
                    events_feed_url = 'http://coderesbgonlinesbs.azurewebsites.net/api/feeds/leagues/' + id + '/liveEvents';
                else:
                    events_feed_url = 'http://coderesbgonlinesbs.azurewebsites.net/api/feeds/leagues/' + id + '/nonLiveEvents'

                print(events_feed_url)
                if not os.path.exists(queue_downloader_path):
                    os.makedirs(queue_downloader_path)

                try:
                    with requests.get(events_feed_url, stream=True, headers=headers) as r:
                        r.raise_for_status()

                        with open(queue_downloader_path + "events-" + id + ".json", 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192): 
                                f.write(chunk)

                        event_feeds.append("events-" + id + ".json")
                except (Exception) as ex:
                    pass
    except (Exception) as ex:
        pass

# Delete temporary files that have been downloaded
#if os.path.exists("sports.json"):
#  os.remove("sports.json")

#if os.path.exists("tournaments.json"):
#  os.remove("tournaments.json")

# Add to queue
if len(event_feeds):
    with open(queue_csv_path, 'a') as fd:
        fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

sio.emit('download_complete', {
    'bookmaker': bookmaker_title,
    'timestamp': timestamp,
    'sport': 'All',
    'type': download_type,
    'feeds': event_feeds
}, namespace='/readers')

sio.sleep(5)
print('Disconnecting!')
sio.disconnect()

print("--- %s seconds ---" % (time.time() - start_time))