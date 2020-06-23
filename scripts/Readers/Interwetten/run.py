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
bookmaker_id = 12
bookmaker_title = 'Interwetten'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
event_feeds = []

bookmaker_updater.init(bookmaker_id, bookmaker_title)

# Extract row from CSV and process it
if os.path.exists(queue_csv_path):
	with open(queue_csv_path, 'r') as file:
		reader = csv.reader(file, delimiter=';')
		for row in reader:
			# timestamp;sports;type;files(separated by comma)
			folder_path = queue_path + row[2] + '/' + row[0] + '/'
			if os.path.exists(folder_path):
				files = row[3].split(',')
				if len(files) > 0:
					for file in files:
						file_path = folder_path + file
						if os.path.exists(file_path):
							print('Processing ' + file)
							items = ijson.items(open(file_path, 'r', encoding="utf-8"), 'sport');
							for item in items:
								sport = item.get('name')
								leagues = item.get('leagues')

								if leagues:
									for league in leagues:
										tournament = league.get('name')
										events = league.get('events')

										for event in events:
											try:
												bookmaker_event = BookmakerEvent.BookmakerEvent()
												event_name = event.get('name')
												print(bookmaker_title + ' :: Processing API event: ' + event_name)

												date = ''
												_datetime = datetime.strptime(event.get('start'), '%Y-%m-%dT%H:%M:%SZ')
												if _datetime:
													_datetime = _datetime + timedelta(hours=2)
													date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

												teams = []
												competitors = event.get('competitors')

												if competitors:
													i = 0
													for competitor in competitors:
														_team = BookmakerEventTeam.BookmakerEventTeam()

														_team.title = competitor.get('name')
														_team.local = competitor.get('pos') == 1 or i == 0

														checkTeamMembers(sport, _team)
														teams.append(_team)

														i += 1

												bookmaker_event.event_id = event.get('id')
												bookmaker_event.title = event_name
												bookmaker_event.tournament = tournament
												bookmaker_event.sport = sport
												bookmaker_event.date = date

												odds = []
												markets = event.get('m')

												for market in markets:
													selections = market.get('o')

													if selections:
														outcomes = []
														for selection in selections:
															bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

															bookmaker_odd_outcome.outcome_id = selection.get('id')
															bookmaker_odd_outcome.title = selection.get('name') if selection.get('name') else selection.get('tip')
															bookmaker_odd_outcome.decimal = selection.get('odd')

															outcomes.append(bookmaker_odd_outcome)

														odd = BookmakerOdd.BookmakerOdd()

														odd.id = market.get('id')
														odd.title = market.get('name')
														odd.outcomes = outcomes

														odds.append(odd)

												bookmaker_event.odds = odds

												# Get teams from markets if array is empty
												if len(teams) == 0:
													for odd in odds:
														i = 0
														for outcome in odd.outcomes:
															team = BookmakerEventTeam.BookmakerEventTeam()

															team.title = outcome.title
															team.local = i == 0

															checkTeamMembers(sport, team)

															teams.append(team)
															i += 1
														break

												bookmaker_event.teams = teams
												bookmaker_updater.processEvent(bookmaker_event)
											except (Exception) as ex:
												print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))