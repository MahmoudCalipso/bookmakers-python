import ijson
import requests
import time
import os
from os import walk
import csv
import sys
import re
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater
import socket
import json

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

start_time = time.time()
timestamp = str(int(time.time()));
bookmaker_id = 15
bookmaker_title = 'Vbet'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';

if len(sys.argv) > 3:
    bookmaker_updater.init(bookmaker_id, bookmaker_title)
    folder_path = queue_path + sys.argv[1] + '/' + sys.argv[2] + '/'
    live = sys.argv[2] == 'live'
    started_at = sys.argv[3]
    if os.path.exists(folder_path):
        files = []
        for (dirpath, dirnames, filenames) in walk(folder_path):
            files.extend(filenames)
            break
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

			# Delete download folder
			shutil.rmtree(folder_path)

			# local host IP '127.0.0.1' 
			host = '127.0.0.1'

			# Define the port on which you want to connect 
			port = 12345

			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

			# connect to server on local computer 
			s.connect((host,port)) 

			# message you send to server 
			message = json.dumps({
				'message': 'read_complete',
				'data': {
				    'bookmaker_id': bookmaker_id,
					'bookmaker_title': bookmaker_title,
					'timestamp': timestamp,
					'live': live,
					'started_at': started_at
			    }
			})

			# message sent to server 
			s.send(message.encode('utf8'))

			s.close()

print("--- %s seconds ---" % (time.time() - start_time))