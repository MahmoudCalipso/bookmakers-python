import requests
import time
import os
from os import walk
import shutil
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
bookmaker_id = 13
bookmaker_title = 'MarathonBet'
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
                    root = ET.parse(file_path).getroot()
                    sports = root.findall('sport')

                    for sport_node in sports:
                        sport = sport_node.attrib['name']
                        groups = sport_node.findall('groups')[0].findall('group')

                        for group in groups:
                            tournament = group.attrib['name']
                            events = group.findall('events')[0].findall('event')

                            for event in events:
                                try:
                                    bookmaker_event = BookmakerEvent.BookmakerEvent()
                                    event_name = event.attrib['name']
                                    date = ''
                                    _datetime = datetime.strptime(event.attrib['date'], '%Y-%m-%dT%H:%M:%SZ')

                                    if _datetime:
                                        _datetime = _datetime + timedelta(hours=2)
                                        date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                    #print(bookmaker_title + ' :: Processing API event: ' + event_name)

                                    teams = []
                                    members = event.findall('members')

                                    if len(members) > 0:
                                        members = members[0].findall('member')
                                        for team in members:
                                            _team = BookmakerEventTeam.BookmakerEventTeam()

                                            _team.title = team.attrib['name']
                                            _team.local = team.attrib['selkey'] == 'HOME'

                                            checkTeamMembers(sport, _team)
                                            teams.append(_team)
                                    else:
                                        _teams = event.findall('teams')

                                        if len(_teams) > 0:
                                            # TEAM 1
                                            team1 = _teams[0].findall('team1')[0]
                                            _team = BookmakerEventTeam.BookmakerEventTeam()

                                            _team.title = team.attrib['name']
                                            _team.local = team.attrib['selkey'] == 'HOME'

                                            checkTeamMembers(sport, _team)
                                            teams.append(_team)

                                            # TEAM 2
                                            team2 = _teams[0].findall('team2')[0]
                                            _team = BookmakerEventTeam.BookmakerEventTeam()

                                            _team.title = team.attrib['name']
                                            _team.local = team.attrib['selkey'] == 'HOME'

                                            checkTeamMembers(sport, _team)
                                            teams.append(_team)

                                    bookmaker_event.event_id = event.attrib['eventId']
                                    bookmaker_event.title = event_name
                                    bookmaker_event.tournament = tournament
                                    bookmaker_event.sport = sport
                                    bookmaker_event.date = date

                                    odds = []
                                    markets = event.findall('markets')[0].findall('market')

                                    for market in markets:
                                        outcomes = []
                                        selections = market.findall('sel')

                                        for selection in selections:
                                            bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

                                            bookmaker_odd_outcome.outcome_id = selection.attrib['coeffId']
                                            bookmaker_odd_outcome.title = selection.attrib['name']
                                            bookmaker_odd_outcome.decimal = float(selection.attrib['coeff'])

                                            outcomes.append(bookmaker_odd_outcome)

                                        odd = BookmakerOdd.BookmakerOdd()

                                        odd.id = market.attrib['event_id']
                                        odd.title = market.attrib['name']
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

                                                checkTeamMembers(keyword_sport, team)

                                                teams.append(team)
                                                i += 1
                                            break

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