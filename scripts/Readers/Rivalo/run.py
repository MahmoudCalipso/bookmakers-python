import requests
import time
import os
import csv
import sys
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater
import socketio

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
bookmaker_id = 16
bookmaker_title = 'Rivalo'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
xml_teams = {}

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

sio.emit('read_complete', {
    'bookmaker_id': bookmaker_id,
    'bookmaker_title': bookmaker_title
}, namespace='/seeder')

sio.sleep(5)
print('Disconnecting!')
sio.disconnect()

print("--- %s seconds ---" % (time.time() - start_time))