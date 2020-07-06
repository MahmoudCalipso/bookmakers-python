import ijson
import requests
import time
import os
from os import walk
import csv
import sys
import re
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater
import socket
import json

EVENT_CHAMPIONSHIP_WINNER = 'Winner'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def str_repeat(str, multiplier):
    return str * multiplier

def filterTeams(sport, teams):
    if sport.find('esport') > -1:
        for team in teams:
            matches = []

            # Check if this team belongs to a E-sport category. It should match this pattern: Title (Nickname)
            matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

            if matches and not matches.group(1):
                team.title = matches.group(1).strip() + ' eSports'

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
bookmaker_id = 2
bookmaker_title = 'Betsson'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
event_feeds = []

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
					events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'el.item')
					for event in events:
						try:
							bookmaker_event = BookmakerEvent.BookmakerEvent()
							sport = event.get('cn')
							tournament = event.get('scn')
							date = ''
							_datetime = datetime.strptime(event.get('sd'), '%Y-%m-%dT%H:%M:%SZ')
							if _datetime:
								_datetime = _datetime + timedelta(hours=1)
								date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

							event_name = event.get('en')
							event_name_teams = []

							if event_name.find('vs') > -1:
								event_name_teams = event_name.split(' vs ')
							elif event_name.find('-') > -1:
								event_name_teams = event_name.split(' - ')

							if len(event_name_teams) == 2:
								event_name = event_name_teams[0] + ' vs ' + event_name_teams[1]

							print(bookmaker_title + ' :: Processing API event: ' + event_name)

							teams = []
							epl = event.get('epl')

							if epl:
								i = 0
								for team in epl:
									_team = BookmakerEventTeam.BookmakerEventTeam()

									_team.title = team.get('pn')
									_team.local = i == 0

									checkTeamMembers(sport, _team)
									teams.append(_team)

									i += 1


							filterTeams(sport, teams)

							bookmaker_event.event_id = event.get('ei')
							bookmaker_event.title = event_name
							bookmaker_event.tournament = tournament
							bookmaker_event.sport = sport
							bookmaker_event.date = date
							bookmaker_event.teams = teams

							odds = []
							ml = event.get('ml')

							if ml:
								for market in ml:
									msl = market.get('msl')
									outcomes = []

									if msl:
										for outcome in msl:
											bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

											bookmaker_odd_outcome.outcome_id = outcome.get('msi')
											bookmaker_odd_outcome.title = outcome.get('mst')
											bookmaker_odd_outcome.decimal = outcome.get('msp')

											# Deep link parameters
											bookmaker_odd_outcome.deep_link = {
												'eventId': event.get('ei'),
												'eventTagId': event.get('eit'),
												'marketId': market.get('mi'),
												'marketTagId': market.get('mit'),
												'selectionId': outcome.get('msi'),
												'selectionTagId': outcome.get('msit'),
												'categoryId': event.get('ci'),
												'betGroupId': market.get('bgi'),
											}

											outcomes.append(bookmaker_odd_outcome)

											odd = BookmakerOdd.BookmakerOdd()

											odd.title = market.get('bggn') + ' - ' + market.get('mn')
											odd.outcomes = outcomes

											odds.append(odd)

							bookmaker_event.odds = odds

							# Check if this event is referring to the championship winner
							now = datetime.now()
							next = now + relativedelta(years=1)
							curr_year = now.strftime('%Y')
							next_year = next.strftime('%Y')

							if (
								(event_name.find(tournament) > -1 or event_name.endswith(curr_year) or event_name.endswith(next_year))
								and len(teams) > 2
							):
								bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

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