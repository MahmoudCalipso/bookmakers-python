import requests
import time
import os
import sys
from datetime import date

sports = {
    'Basketball': [
    	"acb_copa.xml",
        "acb.xml",
        "eurobasket.xml",
        "euroliga.xml",
        "nba.xml",
        "mundialBaloncesto.xml",
    ],
    'Football': [
    	"adelante.xml",
        "amistosos2018.xml",
        "amistososClubes.xml",
        "bundesliga.xml",
        "champions.xml",
        "copaAmerica.xml",
        "copa_rey.xml",
        "europaLeague.xml",
        "LFPAdelante.xml",
        "euro2020.xml",
        "LFP_Rey.xml",
        "LFP_Segunda_B.xml",
        "internationalCup.xml",
        "LFP_Tercera.xml",
        "eurocopa.xml",
        "LFP.xml",
        "ligaArgentina.xml",
        "ligaAustria.xml",
        "ligaBelgica.xml",
        "ligaBrasil.xml",
        "ligaColombiana.xml",
        "ligaEscocesa.xml",
        "ligaHolandesa.xml",
        "liga-mx.xml",
        "ligaPortuguesa.xml",
        "ligaSuiza.xml",
        "ligue1.xml",
        "mls.xml",
        "serieA.xml",
        "premier.xml",
        "supercopa.xml",
        "UEFA_nations_league.xml",
        "mundialFemenino.xml",
        "mundial2018.xml",
    ],
    'Motor': [
    	"motociclismo.xml",
        "motor.xml",
    ],
    'American Football': [
    	"nfl.xml",
        "super-bowl.xml",
    ],
    'Tennis': [
    	"tenis.xml"
    ],
    'Rugby': [
    	"rugby.xml"
    ],
    'Boxing': [
    	'boxeo.xml'
    ]
}

is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
    is_live = True

bookmaker_title = 'Sportium';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

for sport in sports:
    feeds = sports[sport]
    print('Beginning feed download for ' + sport)
    if len(feeds):
        for name in feeds:
            if not os.path.exists(queue_downloader_path):
                os.makedirs(queue_downloader_path)
                
            feed_url = 'https://services.sportium.es/feed/xml/' + name
            print('Beginning feed download for ' + feed_url)
            with requests.get(feed_url, stream=True) as r:
                with open(queue_downloader_path + "events-" + sport + "-" + name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): 
                        f.write(chunk)

            event_feeds.append("events-" + sport + "-" + name)

# Add to queue
if len(event_feeds):
	with open(queue_csv_path, 'a') as fd:
	    fd.write(timestamp + ';All;' + download_type + ';' + ",".join(event_feeds) + "\n")

print("--- %s seconds ---" % (time.time() - start_time))