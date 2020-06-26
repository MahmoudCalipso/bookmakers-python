import requests
import time
import os
import sys
from datetime import date
import socketio

sports = {
	'Football': {
		'path': 'Soccer.asp?MarketID=',
		'markets': ["10244","10216","50264","10233","50137","50136","10206","10221","10253","10209","50433","10208","50240","50241","50265","50266","10207","50138","10241","10234","50139","10204","10202","10219","938","10239","10165","10166","10164","50416","50418","50407","50420","50428","10150","50435","50424","50432","10240","10247","10210","50507","10535","1175","760","10235","10246","43","10114","10544","10258","10205","10252","10203","367","476","45","10539","10538","10519","1178","10248","40","4001","10103","50298","50297","10249","10143","10245","50597","50462","78","50479","10201","10540","10257","50425","50426","42","10537","1579","171","50415","50417","50406","50419","50427","46","10520","10534","10259","50273","10251","50598","1181","1279","50127","50502","10542","10536","10223","50134","50135","10238","50261","50262","50263","10149","10212","50404","44","1170","10242","10243","10236","10237","10151","10152","50111","10110","10127","10211","10250","10214","10215","1183","761","10541","10222","10125","1271","344","10106","50471","10142","50464","50457","50130","50131","50455","10141","50279","10230","1094","50373","50374","10229","64","10228","10218","10217","10227","10226","10224","75","10225","1001","50441","1776","981","10111","50405","10220","56"]
	},
	'Basketball': {
		'path': 'Basketball_v2.asp?MarketID=',
		'markets': ["180019","928","929","180180","180021","941","180170","995","180020","932","933","180188","180022","1448","180171","1451","180023","1032","1033","180024","1449","1452","1517","180013","1454","960","181379","181389","181383","181385","180235","180237","180238","181378","181387","181388","181390","181380","181381","181391","181384","181386","181382","1453","1503","180004","1731","1499","1157","86","180026","1518","180165","180163","180164"]
	},
	'Formula1': {
		'path': 'MotorRacing_v2.asp?MarketID=',
		'markets': ["100002"]
	},
	'Moto GP': {
		'path': 'MotorBikes_v2.asp',
		'markets': []
	},
	'E-Sports': {
		'path': 'general.asp?classId=151&MarketID=',
		'markets': ["151012","1510002","1510001","1510027","151010","151009","1510634","1510636"]
	},
	'Rugby League': {
		'path': 'RugbyLeague_v2.asp?MarketID=',
		'markets': ["190004","190009","190054","190334","190015","190028","190032","190052","190002","190011","190017","190063","190556","190053","190336","190083","190019","190016","190102","190064","190006","130","190138","190084","190340","190339","190106","190044","190082","190007","190025","190027","190112"]
	},
	'Rugby Union': {
		'path': 'RugbyUnion_v2.asp?MarketID=',
		'markets': ["80005","80015","80039","80062","80003","80012","80017","80071","80053","80076","80019","80020","80016","80072","80007","80081","76","80013","80027","80503","80112"]
	},
	'Tennis': {
		'path': 'Tennis_v2.asp?MarketID=',
		'markets': ["130099","130108","130068","130066","130104","358","130052","83","84","130107"]
	},
	'Boxing': {
		'path': 'Boxing_v2.asp?MarketID=',
		'markets': ["90","90022","90030","90004","136","148","90101","90041"]
	},
	'Golf': {
		'path': 'Golf_v2.asp?MarketID=',
		'markets': ["1078","70020","119","67","70080","70013"]
	},
	'American Football': {
		'path': 'AmericanFootball_v2.asp?MarketID=',
		'markets': ["120032","1442","936","720","1441","553","128","127"]
	},
	'Australia': {
		'path': 'SpanishSpecials_v2.asp?MarketID=',
		'markets': ["1360001","1360010"]
	},
	'Badminton': {
		'path': 'Badminton_v2.asp?MarketID=',
		'markets': ["940010","940002","940073"]
	},
	'Baseball': {
		'path': 'Baseball_v2.asp?MarketID=',
		'markets': ["160138","160014","1101","1102","160139","1468","160009","1099","1096","1097","1470","1469"]
	},
	'Cricket': {
		'path': 'Cricket_v2.asp?MarketID=',
		'markets': ["1684","30200","30205","60126","347","349","1246","30016"]
	},
	'Cycling': {
		'path': 'Cycling_v2.asp?MarketID=',
		'markets': ["779","1227"]
	},
	'Darts': {
		'path': 'Darts_v2.asp?MarketID=',
		'markets': ["150108","150127","150133","150178","150183","150015","150145","150134","150135","150146","150125","150128","150129","150126","150041","150116","150131","150013","150136","150025","703","150030","150121","150122","150137","339","150012","150117"]
	},
	'Floorball': {
		'path': 'Floorball_v2.asp?MarketID=',
		'markets': []
	},
	'Greyhounds': {
		'path': 'Greyhounds.asp?MarketID=',
		'markets': ["738","40300","131","702"]
	},
	'Handball': {
		'path': 'Handball_v2.asp?MarketID=',
		'markets': ["780008"]
	},
	'Horse Racing': {
		'path': 'HorseRacingWEW.asp?MarketID=',
		'markets': ["20061","20062","20069","20207","20070","20208","20073","20204","20203","20072","20015","407","20039","20206","20205","20060","81","1744"]
	},
	'Ice Hockey': {
		'path': 'IceHockey_v2.asp?MarketID=',
		'markets': ["1553","1530","170051","972"]
	},
	'Indy': {
		'path': 'Indy_v2.asp?MarketID=',
		'markets': []
	},
	'Italy': {
		'path': 'ItalianSpecials_v2.asp?MarketID=',
		'markets': []
	},
	'Motor': {
		'path': 'MotorBikes_v2.asp?MarketID=',
		'markets': []
	},
	'Nascar': {
		'path': 'Nascar_v2.asp?MarketID=',
		'markets': ["806"]
	},
	'North America': {
		'path': 'NorthAmericanSpecials_v2.asp?MarketID=',
		'markets': ["1290001","1290010"]
	},
	'Other Motosport': {
		'path': 'A1_v2.asp?MarketID=',
		'markets': []
	},
	'World Rally': {
		'path': 'Rally_v2.asp?MarketID=',
		'markets': []
	},
	'Snooker': {
		'path': 'Snooker_v2.asp?MarketID=',
		'markets': ["140125","140126","140148","140158","140152","140156","140155","140159","140162","140163","140164","140165","140160","140161","140157","140149","140150","140153","140154","140151","140019","140005","140234","82"]
	},
	'Squash': {
		'path': 'general.asp?classId=107&MarketID=',
		'markets': []
	},
	'Supercars': {
		'path': 'Cart_v2.asp?MarketID=',
		'markets': ["1140006"]
	},
	'Sweden': {
		'path': 'SwedishSpecials_v2.asp?MarketID=',
		'markets': []
	},
	'Table Tennis': {
		'path': 'TableTennis_v2.asp?MarketID=',
		'markets': ["920008","920000","920014"]
	},
	'Touring Cars': {
		'path': 'TouringCars_v2.asp?MarketID=',
		'markets': []
	},
	'Trotting': {
		'path': 'Trotting_v2.asp?MarketID=',
		'markets': ["880002","880001"]
	},
	'United Kingdom': {
		'path': 'Specials_v2.asp?MarketID=',
		'markets': ["1064","50010"]
	},
	'US Horse Racing': {
		'path': 'USHorseRacing_v2.asp?MarketID=',
		'markets': ["73000"]
	},
	'Volley': {
		'path': 'VolleyBall_v2.asp?MarketID=',
		'markets': ["910201","910209","910218","910206","910204","910202","910217","910000","910213"]
	},
	'World': {
		'path': 'GlobalSpecials_v2.asp?MarketID=',
		'markets': ["1370010"]
	},
	'Futsal': {
		'path': 'Futsal_v2.asp?MarketID=',
		'markets': ["830002", "830001", "830000"]
	},
	'Netball': {
		'path': 'Netball_v2.asp?MarketID=',
		'markets': ["1470183"]
	}
}

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

bookmaker_title = 'Bet365';
download_type = 'live' if is_live else 'prematch';

start_time = time.time()
timestamp = str(int(time.time()));
queue_path = '../../../queues/Downloaders/'
queue_csv_path = queue_path + bookmaker_title + '/queue.csv';
queue_downloader_path = queue_path + bookmaker_title + '/' + download_type + '/' + timestamp + '/';
event_feeds = []

for sport in sports:
	config = sports[sport]
	print('Beginning feed download for ' + sport)
	if len(config['markets']):
		for market_id in config['markets']:
			feed_url = 'http://oddsfeed3.bet365.com/' + config['path'] + market_id

			if is_live:
				feed_url += '&InRunning=1'

			if not os.path.exists(queue_downloader_path):
				os.makedirs(queue_downloader_path)

			print(feed_url)
			with requests.get(feed_url, stream=True) as r:
				r.raise_for_status()

				with open(queue_downloader_path + "events-" + sport + "-" + market_id +".xml", 'wb') as f:
					for chunk in r.iter_content(chunk_size=8192): 
						f.write(chunk)


			event_feeds.append("events-" + sport + "-" + market_id +".xml")

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