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
MARKETS = {
    '1': '1x2',
    '10': 'Double chance',
    '11': 'Draw no bet',
    '14': 'Handicap {hcp}',
    '16': 'Handicap',
    '18': 'Total',
    '29': 'Both teams to score',
    '186': 'Winner',
    '187': 'Game handicap',
    '189': 'Total games',
    '219': 'Winner (incl. overtime)',
    '223': 'Handicap (incl. overtime)',
    '225': 'Total (incl. overtime)',
    '237': 'Point handicap',
    '238': 'Total points',
    '251': 'Winner (incl. extra innings)',
    '258': 'Total (incl. extra innings)',
    '314': 'Total sets',
    '327': 'Map handicap',
    '340': 'Winner (incl. super over)',
    '493': 'Frame handicap',
    '534': 'Championship free text market',
    '535': 'Short term free text market',
    '536': 'Free text multiwinner market',
    '538': 'Head2head (1x2)',
    '539': 'Head2head',
    '559': 'Free text market'
}

def str_repeat(str, multiplier):
    return str * multiplier

def filterTeams(sport, teams):
    if sport == 'Football':
        for team in teams:
            matches = []

            # Check if this team belongs to a E-sport category. It should match this pattern: Title (Nickname)
            matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)

            if matches and not matches.group(1):
                matches = re.search('(?![\s.]+$)([0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*)\((?![\s.]+$)[0-9a-zA-ZÀ-ÖØ-öø-ÿ\-\_\,\s.]*\)', team.title)
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

start_time = time.time()
timestamp = str(int(time.time()));
bookmaker_id = 17
bookmaker_title = 'Betmotion'
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
            live = row[2] == 'live'
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
                                    categories = item.get('Categories')
                                    sport = item.get('Name')

                                    if categories:
                                        for category in categories:
                                            #sport = category.get('Name')
                                            championships = category.get('Championships')
                                            if championships:
                                                for championship in championships:
                                                    tournament = championship.get('Name')
                                                    events = championship.get('Events')
                                                    if events:
                                                        for event in events:
                                                            bookmaker_event = BookmakerEvent.BookmakerEvent()
                                                            event_name = event.get('EventName')
                                                            #print(bookmaker_title + ' :: Processing API event: ' + event_name)

                                                            if event_name.find('vs.') > -1:
                                                                _teams = event_name.split('vs.')

                                                                if len(_teams) == 2:
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
                                                            _datetime = datetime.strptime(event.get('EventDate'), '%Y-%m-%dT%H:%M:%S')
                                                            if _datetime:
                                                                date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                                            bookmaker_event.event_id = event.get('EventId')
                                                            bookmaker_event.title = event_name
                                                            bookmaker_event.tournament = tournament
                                                            bookmaker_event.sport = sport
                                                            bookmaker_event.date = date

                                                            odds = []

                                                            # Get odds from API
                                                            event_feed_url = 'http://dataexport-uof-betmotion.biahosted.com/Export/GetMarkets?importerId=2919&eventId=' + str(event.get('EventId'))
                                                            # Get JSON file from API URL
                                                            response = requests.get(event_feed_url, timeout=30)

                                                            if response.text:
                                                                event_json_path = bookmaker_title + "-event.json"
                                                                file = open(event_json_path, "wb")
                                                                file.write(response.text.encode('utf-8'))
                                                                file.close()

                                                                odds = []
                                                                markets = ijson.items(open(event_json_path, 'r', encoding="utf-8"), 'item')

                                                                for market in markets:
                                                                    odd = BookmakerOdd.BookmakerOdd()
                                                                    outcomes = []
                                                                    selections = market.get('Selections')

                                                                    if selections:
                                                                        for selection in selections:
                                                                            is_enabled = selection.get('IsEnabled')
                                                                            if is_enabled:
                                                                                outcome_title = selection.get('Name')

                                                                                if len(teams) > 0:
                                                                                    outcome_title = outcome_title.replace('{$competitor1}', teams[0].title)
                                                                                    outcome_title = outcome_title.replace('{$competitor2}', teams[0].title)

                                                                                outcome_title = outcome_title.replace('{hcp}', market.get('SpecialOddsValue'))
                                                                                outcome_title = outcome_title.replace('{+hcp}', market.get('SpecialOddsValue'))
                                                                                outcome_title = outcome_title.replace('{-hcp}', market.get('SpecialOddsValue'))
                                                                                outcome_title = outcome_title.replace('{total}', market.get('SpecialOddsValue'))

                                                                                bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

                                                                                bookmaker_odd_outcome.outcome_id = selection.get('SelectionId')
                                                                                bookmaker_odd_outcome.title = outcome_title
                                                                                bookmaker_odd_outcome.decimal = selection.get('Price')

                                                                                outcomes.append(bookmaker_odd_outcome)

                                                                    market_type_id = str(market.get('MarketTypeid'))
                                                                    market_title = MARKETS[market_type_id] if market_type_id in MARKETS else market.get('Name')
                                                                    market_title = market_title.replace('{hcp}', market.get('SpecialOddsValue'))

                                                                    odd.title = market_title
                                                                    odd.outcomes = outcomes

                                                                    odds.append(odd)

                                                            filterTeams(sport, teams)

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
                                                            bookmaker_event.live = live
                                                            
                                                            bookmaker_updater.processEvent(bookmaker_event)

                                except (Exception) as ex:
                                    print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))