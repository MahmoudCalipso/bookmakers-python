import requests
import time
import os
import gzip
import shutil

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue.csv';
queue_downloader_path = queue_path + 'Bethard/' + timestamp + '/';
event_feeds = []

print('-- Beginning events feed download...')
events_feed_url = 'http://bethardxml.sbtech.com/lines.aspx?OddsStyle=DECIMAL&IncludeLinesIDs=true';
headers = {
	'Accept-Encoding': 'deflate, gzip'	
}
response = requests.get(events_feed_url, headers=headers)

if response.text:
    if not os.path.exists(queue_downloader_path):
        os.makedirs(queue_downloader_path)

    file = open("bethard.xml.gz", "wb")
    file.write(response.content)
    file.close()

    file = open(queue_downloader_path + "events.xml", "wb")
    file.close()

    with gzip.open("bethard.xml.gz", "rb") as f_in:
    	with open(queue_downloader_path + "events.xml", "wb") as f_out:
    		shutil.copyfileobj(f_in, f_out);
    		event_feeds.append("events.xml")

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write('Bethard;' + timestamp + ';All;prematch;' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))