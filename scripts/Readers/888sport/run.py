import ijson
import requests
import time
import os
import csv
import sys
import re
from fractions import Fraction
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater

EVENT_CHAMPIONSHIP_WINNER = 'Winner'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def filterTeams(sport, teams):
    if sport.find('esport') > -1 or sport == 'Football':
        for team in teams:
            matches = []

            # Check if this team belongs to a E-sport category. It should match this pattern: Title (Nickname)
            if sport == 'Football':
                matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

                if matches and matches.group(1) and team.title.find('eSports') > -1:
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
bookmaker_id = 9
bookmaker_title = '888sport'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
event_feeds = []
event_feed_url = None

bookmaker_updater.init(bookmaker_id, bookmaker_title)

# Extract row from CSV and process it
if os.path.exists(queue_csv_path):
    with open(queue_csv_path, 'r') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            # timestamp;sports;type;files(separated by comma)
            live = row[2] == 'live'
            event_feed_url = 'http://www.smart-feeds.com/getfeeds.aspx?Param=betoffer/live/event/{id}' if row[2] == 'live' else 'http://www.smart-feeds.com/getfeeds.aspx?Param=betoffer/event/{id}'
            folder_path = queue_path + row[2] + '/' + row[0] + '/'
            if os.path.exists(folder_path):
                files = row[3].split(',')
                if len(files) > 0:
                    for file in files:
                        file_path = folder_path + file
                        if os.path.exists(file_path):
                            #print('Processing ' + file)
                            events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'events.item');
                            for event in events:
                                try:
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
                                            _datetime = datetime.strptime(event.get('start'), '%Y-%m-%dT%H:%MZ')
                                            _datetime = _datetime - timedelta(hours=2)
                                        else:
                                            _datetime = datetime.strptime(event.get('start'))

                                        if _datetime:
                                            date = _datetime.strftime(MYSQL_DATETIME_FORMAT)
                                            #print(bookmaker_title + ' :: Processing API event: ' + event_name)

                                            teams = []

                                            if event.get('homeName') and event.get('awayName'):
                                                participants = [event.get('homeName'), event.get('awayName')]
                                                i = 0

                                                for participant in participants:
                                                    _team = BookmakerEventTeam.BookmakerEventTeam()

                                                    _team.title = participant.strip()
                                                    _team.local = i == 0
                                                    checkTeamMembers(sport, _team)

                                                    teams.append(_team)
                                                    i += 1

                                            filterTeams(sport, teams)

                                            bookmaker_event.event_id = event.get('id')
                                            bookmaker_event.title = event_name
                                            bookmaker_event.tournament = tournament
                                            bookmaker_event.sport = sport
                                            bookmaker_event.date = date
                                            bookmaker_event.teams = teams

                                            # Get odds from API
                                            event_feed_url = event_feed_url.replace('{id}', str(event.get('id')))
                                            # Get JSON file from API URL
                                            response = requests.get(event_feed_url, timeout=30)

                                            if response.text:
                                                event_json_path = bookmaker_title + "-event.json"
                                                file = open(event_json_path, "wb")
                                                file.write(response.text.encode('utf-8'))
                                                file.close()

                                                odds = []
                                                markets = ijson.items(open(event_json_path, 'r', encoding="utf-8"), 'betoffers.item')

                                                for market in markets:
                                                    if market.get('outcomes'):
                                                        outcomes = []

                                                        for outcome in market.get('outcomes'):
                                                            if not outcome.get('oddsFractional'):
                                                                continue

                                                            bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()
                                                            title = outcome.get('englishLabel')

                                                            if title == 'Over' or title == 'Under':
                                                                title += ' ' + (outcome.get('line') / 1000)

                                                            odds_fractional = Fraction(outcome.get('oddsFractional'))
                                                            bookmaker_odd_outcome.outcome_id = outcome.get('id')
                                                            bookmaker_odd_outcome.title = title
                                                            bookmaker_odd_outcome.decimal = float(odds_fractional)

                                                            outcomes.append(bookmaker_odd_outcome)

                                                        odd = BookmakerOdd.BookmakerOdd()

                                                        odd.title = market.get('criterion')['englishLabel']
                                                        odd.outcomes = outcomes

                                                        odds.append(odd)

                                                bookmaker_event.odds = odds

                                            # Check if this event is referring to the championship winner
                                            if event_name.find(' - ') == -1 and len(teams) > 2:
                                                bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

                                            bookmaker_updater.processEvent(bookmaker_event)

                                except (Exception) as ex:
                                    print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))