import requests
import time
import os
import csv
import sys
import re
import xml.etree.ElementTree as ET
from fractions import Fraction
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
    if sport.find('esport') > -1 or sport == 'Soccer':
        for team in teams:
            matches = []

            # Check if this team belongs to a E-sport category. It should match this pattern: Title (Nickname)
            if sport == 'Soccer':
                matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

                if matches and matches.group(1) and team.title.find('Esports') > -1:
                    team.title = matches.group(1).strip() + ' eSports'
            else:
                matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

                if matches and matches.group(1):
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
bookmaker_id = 11
bookmaker_title = 'Bet365'
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
							sport = ET.parse(file_path).getroot()
							sport_title = sport.get('Name')
							tournaments = sport.findall('EventGroup')

							for tournament in tournaments:
								if 'Name' not in tournament.attrib:
									continue

								tournament_title = tournament.attrib['Name']
								events = tournament.findall('Event')

								for event in events:
									try:
										date = ''
										teams = []
										event_name = event.attrib['Name']
										event_name_teams = []

										bookmaker_event = BookmakerEvent.BookmakerEvent()

										if event_name.find(' v ') > -1:
											event_name_teams = event_name.split(' v ')
										elif event_name.find(' vs ') > -1:
											event_name_teams = event_name.split(' vs ')
										elif event_name.find(' @ ') > -1:
											event_name_teams = event_name.split(' @ ')
											event_name_teams = reversed(event_name_teams)

										if len(event_name_teams) > 0:
											i = 0
											for event_name_team in event_name_teams:
												_team = BookmakerEventTeam.BookmakerEventTeam()

												_team.title = event_name_team
												_team.local = i == 0

												checkTeamMembers(sport_title, _team)

												teams.append(_team)
												i += 1

											event_name = teams[0].title + ' vs ' + teams[1].title

										#print(bookmaker_title + ' :: Processing API event: ' + event_name)
										
										# Add 1 hour as it's UTC time
										_time = event.attrib['StartTime']

										if len(_time) > 0:
											_datetime = datetime.strptime(_time, '%d/%m/%y %H:%M:%S')

											if _datetime:
												_datetime = _datetime + timedelta(hours=1)
												date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

										bookmaker_event.event_id = event.attrib['ID']
										bookmaker_event.title = event_name
										bookmaker_event.tournament = tournament_title
										bookmaker_event.sport = sport_title
										bookmaker_event.date = date

										odds = []
										markets = event.findall('Market')

										if markets:
											for market in markets:
												odd = BookmakerOdd.BookmakerOdd()
												outcomes = []
												participants = market.findall('Participant')

												if participants:
													for outcome in participants:
														bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()
														outcome_title = outcome.attrib['Name']

														if 'Handicap' in market.attrib:
															outcome_title += ' ' + market.attrib['Handicap']
														elif 'Handicap' in outcome.attrib:
															outcome_title += ' ' + outcome.attrib['Handicap']

														try:
															decimal = float(outcome.attrib['OddsDecimal'])
														except:
															decimal = 0

														bookmaker_odd_outcome.outcome_id = outcome.attrib['ID'] if 'ID' in outcome.attrib else None
														bookmaker_odd_outcome.title = outcome_title
														bookmaker_odd_outcome.decimal = decimal

														if 'AVG' in outcome.attrib:
															deep_link = {'AVG': outcome.attrib['AVG']}
															bookmaker_odd_outcome.deep_link = deep_link

														outcomes.append(bookmaker_odd_outcome)

												odd.id = market.attrib['FID']
												odd.title = market.attrib['Name']
												odd.outcomes = outcomes

												odds.append(odd)

										bookmaker_event.odds = odds

										filterTeams(sport_title, teams)

										# Get teams from markets if array is empty
										if len(teams) == 0:
											for odd in odds:
												i = 0
												for outcome in odd.outcomes:
													team = BookmakerEventTeam.BookmakerEventTeam()

													team.title = outcome.title
													team.local = i == 0

													checkTeamMembers(sport_title, team)

													teams.append(team)
													i += 1
												break

										bookmaker_event.teams = teams

										# Check if this event is referring to the championship winner
										if event_name.find(' vs ') == -1 and len(teams) > 2:
											bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

										bookmaker_event.live = live

										bookmaker_updater.processEvent(bookmaker_event)
									except (Exception) as ex:
										print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

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
		'bookmaker_title': bookmaker_title
    }
})

# message sent to server 
s.send(message.encode('utf8'))

s.close()

print("--- %s seconds ---" % (time.time() - start_time))