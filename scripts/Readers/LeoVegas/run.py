import ijson
import requests
import time
import os
from os import walk
import shutil
import csv
import sys
import re
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
bookmaker_id = 8
bookmaker_title = 'LeoVegas'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
event_feeds = []
event_feed_url = None

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
                    events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'events.item');
                    for event in events:
                        try:
                            live = event.get('liveData')
                            event = event.get('event')
                            bookmaker_event = BookmakerEvent.BookmakerEvent()
                            event_name = event.get('englishName')
                            sport = event.get('path')[0]['englishName']

                            # Check if it's Motorsport
                            if sport == 'Motorsport':
                                sport = event.get('path')[1]['englishName']
                                tournament = event_name
                            else:
                                country = event.get('path')[1]['englishName'] + ' ' if len(event.get('path')) == 3 else ''
                                tournament = country + event.get('group')

                            # Check if this event should be skipped
                            if len(sport) > 0:
                                _datetime = None
                                date = ''

                                if event.get('start').endswith('Z'):
                                    # UTC Time
                                    _datetime = datetime.strptime(event.get('start'), '%Y-%m-%dT%H:%M:%SZ')
                                    _datetime = _datetime + timedelta(hours=2)
                                else:
                                    _datetime = datetime.strptime(event.get('start'))

                                if _datetime:
                                    date = _datetime.strftime(MYSQL_DATETIME_FORMAT)
                                    #print(bookmaker_title + ' :: Processing API event: ' + event_name)

                                    teams = []

                                    if event.get('participants'):
                                        for participant in event.get('participants'):
                                            _team = BookmakerEventTeam.BookmakerEventTeam()

                                            _team.title = participant.get('name').strip()
                                            _team.local = participant.get('home')
                                            checkTeamMembers(sport, _team)

                                            teams.append(_team)

                                    bookmaker_event.event_id = event.get('id')
                                    bookmaker_event.title = event_name
                                    bookmaker_event.tournament = tournament
                                    bookmaker_event.sport = sport
                                    bookmaker_event.date = date
                                    bookmaker_event.teams = teams

                                    # Get odds from API
                                    event_feed_url = 'https://sports-offering.leovegas.com/offering/v2018/es/betoffer/event/' + str(event.get('id'))
                                    event_json_path = bookmaker_title + "-event.json"
                                    with requests.get(event_feed_url, stream=True, timeout=15) as r:
                                        with open(event_json_path, 'wb') as f:
                                            for chunk in r.iter_content(chunk_size=8192): 
                                                # If you have chunk encoded response uncomment if
                                                # and set chunk_size parameter to None.
                                                #if chunk: 
                                                f.write(chunk)

                                    odds = []
                                    markets = ijson.items(open(event_json_path, 'r', encoding="utf-8"), 'betOffers.item')

                                    for market in markets:
                                        if market.get('outcomes'):
                                            outcomes = []

                                            for outcome in market.get('outcomes'):
                                                if not outcome.get('oddsFractional'):
                                                    continue

                                                bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()
                                                title = outcome.get('englishLabel')

                                                if title == 'Over' or title == 'Under':
                                                    title += ' ' + str(outcome.get('line') / 1000)

                                                try:
                                                    odds_fractional = Fraction(outcome.get('oddsFractional'))
                                                    decimal = float(odds_fractional)
                                                except:
                                                    decimal = 0

                                                bookmaker_odd_outcome.outcome_id = str(outcome.get('id'))
                                                bookmaker_odd_outcome.title = title
                                                bookmaker_odd_outcome.decimal = decimal

                                                outcomes.append(bookmaker_odd_outcome)

                                            odd = BookmakerOdd.BookmakerOdd()

                                            odd.title = market.get('criterion')['englishLabel']
                                            odd.outcomes = outcomes

                                            odds.append(odd)

                                        bookmaker_event.odds = odds

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