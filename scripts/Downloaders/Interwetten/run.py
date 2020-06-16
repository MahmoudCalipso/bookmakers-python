import ijson
import requests
import time
import os
import json
import sys

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Interwetten';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

# Auth
print('Retrieving auth token...')
headers = {
    'Content-Type': 'application/json',
}
params = {
	'username': 'scannerbet_7', 
	'password': 'MBJ8MhS&rQ47%F$CUNU76BzME'
}
auth_url = 'https://partner.odds.ws/api/Auth/Login'
response = requests.post(auth_url, json.dumps(params), headers=headers)
file = open("auth.json", "wb")
file.write(response.text.encode('utf-8'))
file.close()

# Get token
token = None
for prefix, event, value in ijson.parse(open('auth.json', 'r')):
	if prefix == 'token':
		token = value;
		break;

if token:
	# Download sports feed
	print('Beginning sports feed download...')
	headers = {
		'Authorization': 'Bearer ' + token,
	}
	sports_feed_url = 'https://partner.odds.ws/api/Sports/EN'
	response = requests.get(sports_feed_url, headers=headers)
	file = open("sports.json", "wb")
	file.write(response.text.encode('utf-8'))
	file.close()

	# Loop sports
	sports = ijson.items(open('sports.json', 'r'), 'items.item');
	for sport in sports:
		name = sport.get('name')
		id = sport.get('id')
		print("Looping sport " + name + " with ID " + str(id))
		print('-- Beginning events feed download...')

		events_feed_url = 'https://partner.odds.ws/api/sport/EN/' + str(id) + '?eventFilter=' + ('Live' if is_live else 'PreMatch') + '&marketFilter=All'
		print(events_feed_url)
		response = requests.get(events_feed_url, headers=headers)

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

#if os.path.exists("tournaments.json"):
#  os.remove("tournaments.json")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write('Interwetten;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))