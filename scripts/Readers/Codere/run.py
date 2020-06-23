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
bookmaker_id = 4
bookmaker_title = 'Codere'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
events_to_process = {}

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
                            items = ijson.items(open(file_path, 'r', encoding="utf-8"), 'item');
                            for item in items:
                                try:
                                    key = item.get('Key')
                                    event = item.get('Value')
                                    event_name = event.get('NodeName')
                                    sport = event.get('Parent3NodeName')
                                    tournament = event.get('ParentNodeName')
                                    participants = event.get('Participants')
                                    date = ''

                                    if key not in events_to_process and participants and len(participants) > 0:
                                        #print(bookmaker_title + ' :: Processing API event: ' + event_name)
                                        teams = []
                                        local_team = None

                                        for participant in participants:
                                            _team = BookmakerEventTeam.BookmakerEventTeam()

                                            _team.title = participant.get('LocalizedNames')['LocalizedValues'][0]['Value']
                                            _team.local = participant.get('IsHome')

                                            checkTeamMembers(sport, _team)

                                            if participant.get('IsHome'):
                                                local_team = _team
                                            else:
                                                teams.append(_team)

                                        if local_team:
                                            # Local team must be always at the beginning of the list
                                            teams.insert(0, local_team)

                                        bookmaker_event = BookmakerEvent.BookmakerEvent()
                                        start_date = event.get('StartDate')

                                        # UTC Time
                                        _datetime = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')
                                        if _datetime:
                                            _datetime = _datetime + timedelta(hours=2)
                                            date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                        bookmaker_event.event_id = key
                                        bookmaker_event.title = event_name
                                        bookmaker_event.tournament = tournament
                                        bookmaker_event.sport = sport
                                        bookmaker_event.date = date
                                        bookmaker_event.teams = teams

                                        # Check if this event is referring to the championship winner
                                        if event_name.find(' - ') == -1 and len(teams) > 2:
                                            bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

                                        events_to_process[key] = bookmaker_event
                                    elif event.get('Odd') and event.get('Parent2NodeId') in events_to_process:
                                        #print(bookmaker_title + ' :: Processing API event: ' + event.get('Parent2NodeName'))
                                        bookmaker_event = events_to_process[event.get('Parent2NodeId')]
                                        odd_index = -1
                                        i = 0

                                        for odd in bookmaker_event.odds:
                                            if odd.title == event.get('ParentNodeName'):
                                                odd_index = i
                                                break
                                            i += 1

                                        outcome = BookmakerOddOutcome.BookmakerOddOutcome()

                                        outcome.outcome_id = event.get('NodeId')
                                        outcome.title = event.get('NodeName')
                                        outcome.decimal = event.get('Odd')

                                        if odd_index == -1:
                                            odd = BookmakerOdd.BookmakerOdd()

                                            odd.title = event.get('ParentNodeName')
                                            odd.outcomes.append(outcome)

                                            bookmaker_event.odds.append(odd)
                                        else:
                                            bookmaker_event.odds[odd_index].outcomes.append(outcome)
                                except (Exception) as ex:
                                    print(bookmaker_title + ' :: Could not process event: ' + str(ex))

for key in events_to_process:
    try:
        print(bookmaker_title + ' :: Processing API event: ' + events_to_process[key].title)
        bookmaker_updater.processEvent(events_to_process[key])
    except (Exception) as ex:
        print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))