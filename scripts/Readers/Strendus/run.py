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
bookmaker_id = 18
bookmaker_title = 'Strendus'
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
                            events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'd.m.item');
                            for event in events:
                                try:
                                    sport = event.get('sn')
                                    tournament = event.get('ltn') if live else event.get('tn')
                                    event_name = event.get('ht') + ' vs ' + event.get('at')

                                    print(bookmaker_title + ' :: Processing API event: ' + event_name)
                                    bookmaker_event = BookmakerEvent.BookmakerEvent()
                                    teams = []

                                    _teams = [event.get('ht'), event.get('at')]

                                    team_local = BookmakerEventTeam.BookmakerEventTeam()
                                    team_local.title = _teams[0].strip()
                                    team_local.local = True
                                    checkTeamMembers(sport, team_local)

                                    team_away = BookmakerEventTeam.BookmakerEventTeam()
                                    team_away.title = _teams[1].strip()
                                    team_away.local = False
                                    checkTeamMembers(sport, team_away)

                                    teams = [team_local, team_away]

                                    date = ''
                                    _datetime = datetime.strptime(event.get('d'), '%Y-%m-%d %H:%M')
                                    if _datetime:
                                        _datetime = _datetime + timedelta(hours=2)
                                        date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                    bookmaker_event.event_id = event.get('mid')
                                    bookmaker_event.title = event_name
                                    bookmaker_event.tournament = tournament
                                    bookmaker_event.sport = sport
                                    bookmaker_event.date = date

                                    odds = []

                                    # Get odds from API
                                    event_feed_url = 'https://sports.strendus.com.mx/rest/FEMobile/GetMatchOdds?Culture=en&affi=49&MatchID=' + str(event.get('mid'))
                                    event_json_path = bookmaker_title + "-event.json"
                                    with requests.get(event_feed_url, stream=True, timeout=15) as r:
                                        with open(event_json_path, 'wb') as f:
                                            for chunk in r.iter_content(chunk_size=8192): 
                                                # If you have chunk encoded response uncomment if
                                                # and set chunk_size parameter to None.
                                                #if chunk: 
                                                f.write(chunk)


                                    odds = []
                                    tabs = ijson.items(open(event_json_path, 'r', encoding="utf-8"), 't.item')

                                    for tab in tabs:
                                        markets = tab.get('o')

                                        for market in markets:
                                            odd = BookmakerOdd.BookmakerOdd()
                                            outcomes = []
                                            selections = market.get('m')

                                            if selections:
                                                for selection in selections:
                                                    bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

                                                    bookmaker_odd_outcome.outcome_id = str(selection.get('id'))
                                                    bookmaker_odd_outcome.title = selection.get('n')
                                                    bookmaker_odd_outcome.decimal = float(selection.get('o'))

                                                    outcomes.append(bookmaker_odd_outcome)

                                            odd.title = market.get('n')
                                            odd.outcomes = outcomes

                                            odds.append(odd)

                                    bookmaker_event.odds = odds
                                    bookmaker_event.teams = teams
                                    bookmaker_event.live = live

                                    bookmaker_updater.processEvent(bookmaker_event)

                                except (Exception) as ex:
                                    print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))