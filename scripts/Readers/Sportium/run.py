import requests
import time
import os
import csv
import sys
import re
import xml.etree.ElementTree as ET
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
bookmaker_id = 6
bookmaker_title = 'Sportium'
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
                            try:
                                sport = file.lstrip('events-')
                                separator_pos = sport.find('-')
                                sport = sport[0:separator_pos]
                                root = ET.parse(file_path).getroot()
                                competitions = root.findall('Competition')

                                for competition in competitions:
                                    tournament = competition.findall('Name')[0].text
                                    events = competition.findall('Event')

                                    for event in events:
                                        try:
                                            bookmaker_event = BookmakerEvent.BookmakerEvent()
                                            teams = []
                                            event_name_teams = []
                                            event_name = event.findall('Match')[0].text

                                            #print(bookmaker_title + ' :: Processing API event: ' + event_name)

                                            if event_name.find('vs') > -1:
                                                event_name_teams = event_name.split(' vs ')
                                            elif event_name.find(' @ ') > -1:
                                                event_name_teams = event_name.split(' @ ')
                                                event_name_teams.reverse()

                                            if len(event_name_teams) > 0:
                                                i = 0
                                                for team in event_name_teams:
                                                    _team = BookmakerEventTeam.BookmakerEventTeam()

                                                    _team.title = team
                                                    _team.local = i == 0

                                                    checkTeamMembers(sport, _team)
                                                    teams.append(_team)

                                                    i += 1

                                            _datetime = datetime.strptime(event.findall('Time')[0].text, '%Y-%m-%d %H:%M:%S')
                                            if _datetime:
                                                _datetime = _datetime + timedelta(hours=2)
                                                date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                            bookmaker_event.event_id = event.attrib['ev_id'] if 'ev_id' in event.attrib else None
                                            bookmaker_event.title = event_name
                                            bookmaker_event.tournament = tournament
                                            bookmaker_event.sport = sport
                                            bookmaker_event.date = date

                                            odds = []
                                            markets = event.findall('Prices')[0].findall('Market')

                                            for market in markets:
                                                title = market.findall('Title')[0].text
                                                odd = BookmakerOdd.BookmakerOdd()
                                                outcomes = []
                                                prices = market.findall('Price')

                                                for price in prices:
                                                    bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

                                                    # Get decimal by regular expression
                                                    decimal_matches = re.search(':([0-9]+\.[0-9]{1,2})', price.text)

                                                    bookmaker_odd_outcome.outcome_id = price.attrib['bet_ref'] if 'bet_ref' in price.attrib else None
                                                    bookmaker_odd_outcome.title = price.attrib['name']
                                                    bookmaker_odd_outcome.decimal = float(decimal_matches.group(1)) if decimal_matches.group(1) else 0

                                                    outcomes.append(bookmaker_odd_outcome)

                                                odd.title = title
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

                                            # Check if this event is referring to the championship winner
                                            now = datetime.now()
                                            next = now + relativedelta(years=1)
                                            curr_year = now.strftime('%Y')
                                            next_year = next.strftime('%Y')

                                            if (
                                                (event_name.find(tournament) > -1 or event_name.endswith(curr_year) or event_name.endswith(next_year) or event_name.endswith(next_year[0:2]))
                                                and len(teams) > 2
                                            ):
                                                bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

                                            bookmaker_event.live = live

                                            bookmaker_updater.processEvent(bookmaker_event)

                                        except (Exception) as ex:
                                            print(bookmaker_title + ' :: Could not process event: ' + str(ex))

                            except:
                                pass

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