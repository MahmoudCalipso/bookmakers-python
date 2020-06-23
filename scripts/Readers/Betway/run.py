import requests
import time
import os
import csv
import sys
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater

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
bookmaker_id = 3
bookmaker_title = 'Betway'
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
                            root = ET.parse(file_path).getroot()
                            events = root.findall('Event')

                            for event in events:
                                try:
                                    date = ''
                                    keywords = event.findall('Keywords')[0].findall('Keyword')
                                    keyword_sport = ''
                                    keyword_tournament = ''
                                    keyword_country = ''
                                    keyword_teams = []
                                    event_name = event.findall('Names')[0].findall('Name')[0].text
                                    tournament = ''
                                    winner_event_found = False

                                    # Extract sport, league and teams from keywords
                                    for keyword in keywords:
                                        type = keyword.attrib['type_cname']

                                        if type == 'sport':
                                            keyword_sport = keyword.text
                                        elif type == 'league':
                                            keyword_tournament = keyword.text
                                        elif type == 'country':
                                            keyword_country = keyword.text
                                        elif type == 'team':
                                            keyword_teams.append(keyword.text)

                                    if len(keyword_teams) == 0:
                                        # Get tournament from event name if this is related to the winner market
                                        if event_name.find(keyword_tournament) > -1 and event_name.find('Winner') > -1:
                                            pos = event_name.find('Winner')
                                            tournament = event_name[0:pos]
                                            winner_event_found = True
                                        else:
                                            tournament = keyword_country + ' ' + keyword_tournament
                                    else:
                                        if keyword_sport == 'Motor Sport':
                                            keyword_sport = keyword_country

                                        tournament = keyword_country + ' ' + keyword_tournament

                                    #print(bookmaker_title + ' :: Processing API event: ' + event_name)
                                    bookmaker_event = BookmakerEvent.BookmakerEvent()
                                    teams = []

                                    if len(keyword_teams) > 0 and len(keyword_teams) == 2:
                                        team1_pos = event_name.find(keyword_teams[0])
                                        team2_pos = event_name.find(keyword_teams[1])

                                        if team1_pos > team2_pos:
                                            keyword_teams = [keyword_teams[1], keyword_teams[0]]

                                        i = 0
                                        for keyword_team in keyword_teams:
                                            team = BookmakerEventTeam.BookmakerEventTeam()

                                            team.title = keyword_team
                                            team.local = i == 0

                                            checkTeamMembers(keyword_sport, team)
                                            teams.append(team)

                                            i += 1

                                        event_name = teams[0].title + ' vs ' + teams[1].title

                                    filterTeams(keyword_sport, teams)

                                    start_at = event.attrib['start_at']
                                    _datetime = datetime.strptime(start_at, '%Y/%m/%dT%H:%M:%SZ UTC')

                                    if _datetime:
                                        _datetime = _datetime + timedelta(hours=2)
                                        date = _datetime.strftime(MYSQL_DATETIME_FORMAT)

                                    bookmaker_event.event_id = event.attrib['id']
                                    bookmaker_event.title = event_name
                                    bookmaker_event.tournament = tournament
                                    bookmaker_event.sport = keyword_sport
                                    bookmaker_event.date = date

                                    odds = []
                                    markets = event.findall('Markets')[0].findall('Market')

                                    if markets:
                                        for market in markets:
                                            title = market.findall('Names')[0].findall('Name')[0].text
                                            odd = BookmakerOdd.BookmakerOdd()
                                            outcomes = []
                                            selections = market.findall('Outcomes')[0].findall('Outcome')

                                            for selection in selections:
                                                bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()
                                                outcome_title = selection.findall('Names')[0].findall('Name')[0].text

                                                if 'handicap' in market.attrib:
                                                    outcome_title += ' ' + market.attrib['handicap']

                                                # Get decimal by regular expression
                                                bookmaker_odd_outcome.outcome_id = selection.attrib['id'] if 'id' in selection.attrib else None
                                                bookmaker_odd_outcome.title = outcome_title
                                                bookmaker_odd_outcome.decimal = float(selection.attrib['price_dec']) if 'price_dec' in selection.attrib else 0

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

                                                checkTeamMembers(keyword_sport, team)

                                                teams.append(team)
                                                i += 1
                                            break

                                    bookmaker_event.teams = teams

                                    # Check if this event is referring to the championship winner
                                    if winner_event_found:
                                        bookmaker_event.replace_title = EVENT_CHAMPIONSHIP_WINNER

                                    bookmaker_updater.processEvent(bookmaker_event)

                                except (Exception) as ex:
                                    print(bookmaker_title + ' :: Could not process event: ' + str(ex))

bookmaker_updater.finish()

print("--- %s seconds ---" % (time.time() - start_time))