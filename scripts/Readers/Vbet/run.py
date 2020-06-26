import ijson
import requests
import time
import os
import csv
import sys
import re
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater
import socketio

EVENT_CHAMPIONSHIP_WINNER = 'Winner'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def checkTeamMembers(sport, team):
	if sport == 'Tennis':
		matches = re.search('(.*)\/(.*)', team.title)

		if matches and matches.group(1) and matches.group(2) and matches.group(1) != matches.group(2):
			members = []
			
			member = BookmakerEventTeamMember.BookmakerEventTeamMember()
			member.title = matches.group(1)
			members.append(member)

			member = BookmakerEventTeamMember.BookmakerEventTeamMember()
			member.title = matches.group(2)
			members.append(member)

			team.members = members

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

sio.connect('http://127.0.0.1:5000', namespaces=['/seeder'])

start_time = time.time()
timestamp = str(int(time.time()));
bookmaker_id = 15
bookmaker_title = 'Vbet'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';

bookmaker_updater.init(bookmaker_id, bookmaker_title)

# Extract row from CSV and process it
if os.path.exists(queue_csv_path):
	with open(queue_csv_path, 'r') as file:
		reader = csv.reader(file, delimiter=';')
		for row in reader:
			# timestamp;sports;type;files(separated by comma)
			live = row[2] == 'live'
			folder_path = queue_path + row[2] + '/' + row[0] + '/'
			if os.path.exists(folder_path):
				files = row[3].split(',')
				if len(files) > 0:
					for file in files:
						file_path = folder_path + file
						if os.path.exists(file_path):
							print('Processing ' + file)
							items = ijson.items(open(file_path, 'r', encoding="utf-8"), 'items.item')

							for item in items:
								sports = item.get('sport')
								for sport_key in sports:
									sport_entry = sports.get(sport_key)
									sport = sport_entry.get('name')
									print(sport)
									regions = sport_entry.get('region')
									for region_key in regions:
										region = regions.get(region_key)
										competitions = region.get('competition')
										for competition_key in competitions:
											competition = competitions.get(competition_key)
											tournament = competition.get('name')
											games = competition.get('game')
											for game_id in games:
												event = games.get(game_id)
												try:
													bookmaker_event = BookmakerEvent.BookmakerEvent()

													teams = []

													if event.get('team2_name'):
														team_local = BookmakerEventTeam.BookmakerEventTeam()
														team_local.title = event.get('team1_name')
														team_local.local = True
														checkTeamMembers(sport, team_local)

														team_away = BookmakerEventTeam.BookmakerEventTeam()
														team_away.title = event.get('team2_name')
														team_away.local = False
														checkTeamMembers(sport, team_away)
														teams = [team_local, team_away]
														event_name = teams[0].title + ' vs ' + teams[1].title
													else:
														event_name = event.get('team1_name')

													event_name = teams[0].title + ' vs ' + teams[1].title

													#print(bookmaker_title + ' :: Processing API event: ' + event_name)

													bookmaker_event.event_id = event.get('id')
													bookmaker_event.title = event_name
													bookmaker_event.tournament = tournament
													bookmaker_event.sport = sport
													bookmaker_event.date = event.get('start_ts')

													odds = []

													# Get odds from API
													event_feed_url = 'https://vbetaffiliates-admin.com/global/feed/json/?language=eng&timeZone=179&filterData%5Bstart_ts%5D=172800&brandId=4&gameid=' + str(event.get('id'))
													event_json_path = bookmaker_title + "-event.json"
													with requests.get(event_feed_url, stream=True, timeout=15) as r:
														with open(event_json_path, 'wb') as f:
															for chunk in r.iter_content(chunk_size=8192): 
																# If you have chunk encoded response uncomment if
																# and set chunk_size parameter to None.
																#if chunk: 
																f.write(chunk)

													markets = ijson.items(open(event_json_path, 'r', encoding="utf-8"), 'markets.item')

													for market in markets:
														odd = BookmakerOdd.BookmakerOdd()
														outcomes = []
														selections = market.get('m')

														if selections:
															for selection in selections:
																bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

																bookmaker_odd_outcome.outcome_id = str(selection.get('event_id'))
																bookmaker_odd_outcome.title = selection.get('name') + (outcome.get('base') if outcome.get('base') else '')
																bookmaker_odd_outcome.decimal = selection.get('price')

																outcomes.append(bookmaker_odd_outcome)

														odd.title = market.get('name')
														odd.outcomes = outcomes

														odds.append(odd)

													bookmaker_event.odds = odds
													bookmaker_event.teams = teams
													bookmaker_event.live = live

													bookmaker_updater.processEvent(bookmaker_event)

												except (Exception) as ex:
													print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

sio.emit('read_complete', {
    'bookmaker_id': bookmaker_id,
    'bookmaker_title': bookmaker_title
}, namespace='/seeder')

sio.sleep(5)
print('Disconnecting!')
sio.disconnect()

print("--- %s seconds ---" % (time.time() - start_time))