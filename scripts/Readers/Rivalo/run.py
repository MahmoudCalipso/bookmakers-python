import requests
import time
import os
from os import walk
import csv
import sys
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater
import socket
import json

EVENT_CHAMPIONSHIP_WINNER = 'Winner'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def filterTeams(sport, teams):
    if sport == 'Football':
        for team in teams:
            matches = []

            # Check if this team belongs to a E-sport category. It should match this pattern: Title (Nickname)
            matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

            if matches and not matches.group(1):
                team.title = matches.group(1) + ' eSports'
            elif team.title.find(' SRL') > -1:
                srl_index = team.title.find(' SRL')
                team.title = team.title[0:srl_index] + ' eSports'

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
bookmaker_id = 16
bookmaker_title = 'Rivalo'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
xml_teams = {}

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
					root = ET.parse(file_path).getroot()

					# Get teams
					teams = root.findall('participant')

					for team in teams:
						xml_teams[team.attrib['id']] = team.findall('name')[0].text

					events = root.findall('event')
					for event in events:
						try:
							date = ''
							sport = event.findall('root-group-name')[0].text
							tournament = event.findall('group-name')[0].text
							participants = event.findall('event-participant')
							teams = []

							if len(participants) > 0:
								for participant in participants:
									participant_id = participant.findall('participant-id')[0].text
									i = 0

									if participant_id in xml_teams:
										_team = BookmakerEventTeam.BookmakerEventTeam()

										_team.title = xml_teams[participant_id]
										_team.local = i == 0

										checkTeamMembers(sport, _team)
										teams.append(_team)
										i += 1

							event_name = event.findall('name')[0].text

							print(bookmaker_title + ' :: Processing API event: ' + event_name)

							bookmaker_event = BookmakerEvent.BookmakerEvent()

							_datetime = datetime.strptime(event.findall('begin')[0].text, '%Y-%m-%dT%H:%M:%S')
							if _datetime:
								date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

							bookmaker_event.event_id = event.attrib['id']
							bookmaker_event.title = event_name
							bookmaker_event.tournament = tournament
							bookmaker_event.sport = sport
							bookmaker_event.date = date

							odds = []
							markets = event.findall('result-set')
							if len(markets) > 0:
								for market in markets:
									odd = BookmakerOdd.BookmakerOdd()
									outcomes = []
									selections = market.findall('result')

									if len(selections):
										for selection in selections:
											outcome = BookmakerOddOutcome.BookmakerOddOutcome()
											choice_param = selection.findall('choice-param')[0].text

											outcome.outcome_id = selection.attrib['id'] if 'id' in selection.attrib else None

											if choice_param == '1' or choice_param == '2' or choice_param in xml_teams:
												if choice_param == '1' or choice_param == '2':
													outcome.title = teams[int(choice_param) - 1].title
												else:
													outcome.title = xml_teams[choice_param].title
											else:
												fixed_param = ''
												params = market.findall('fixed_param')

												if len(params) > 0:
													fixed_param = params[0].text

												outcome.title = choice_param + fixed_param

											outcome.decimal = float(selection.findall('quote')[0].text)

											outcomes.append(outcome)

									odd.title = market.findall('result-type-id')[0].text
									odd.outcomes = outcomes

									odds.append(odd)

							filterTeams(sport, teams)

							bookmaker_event.odds = odds
							bookmaker_event.teams = teams
							bookmaker_event.live = event.findall('live')[0].text == '1'

							# Check if this event is referring to the championship winner
							if event_name.find(' vs ') == -1 and len(teams) > 2:
								bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

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