import ijson
import requests
import time
import os
from os import walk
import shutil
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
bookmaker_id = 18
bookmaker_title = 'Strendus'
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
                    events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'd.m.item');
                    for event in events:
                        try:
                            sport = event.get('sn')
                            tournament = event.get('ltn') if live else event.get('tn')
                            event_name = event.get('ht') + ' vs ' + event.get('at')

                            #print(bookmaker_title + ' :: Processing API event: ' + event_name)
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