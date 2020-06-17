import os
import csv

# Constants
BOOKMAKER_SPORTS_TABLE = 'bookmaker_sports'
BOOKMAKER_TOURNAMENTS_TABLE = 'bookmaker_tournaments'
BOOKMAKER_TEAMS_TABLE = 'bookmaker_teams'
BOOKMAKER_MARKETS_TABLE = 'bookmaker_markets'
EVENTS_TABLE = 'events'
EVENT_TEAMS_TABLE = 'event_teams'
BOOKMAKER_EVENTS_TABLE = 'bookmaker_events'
BOOKMAKER_EVENT_MARKETS_TABLE = 'bookmaker_event_markets'
BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE = 'bookmaker_event_market_outcomes'

# Variables
entities = ['sports', 'tournaments', 'teams', 'markets']
bookmaker_sports = {}
bookmaker_tournaments = {}
bookmaker_teams = {}
bookmaker_markets = {}

sports_maps = {}
tournaments_maps = {}
teams_maps = {}
markets_maps = {}

bookmaker_sports_to_skip = []
bookmaker_tournaments_to_skip = []

processed_files = 0
events_processed = 0
processedEntities = {
    BOOKMAKER_SPORTS_TABLE: 0,
    BOOKMAKER_TOURNAMENTS_TABLE: 0,
    BOOKMAKER_TEAMS_TABLE: 0,
    BOOKMAKER_MARKETS_TABLE: 0,
    EVENTS_TABLE: 0,
    EVENT_TEAMS_TABLE: 0,
    BOOKMAKER_EVENTS_TABLE: 0,
    BOOKMAKER_EVENT_MARKETS_TABLE: 0,
    BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE: 0
}

processed_bookmaker_sports = []
processed_bookmaker_tournaments = []
processed_bookmaker_teams = []
processed_bookmaker_markets = []
processed_bookmaker_bookmaker_markets = []
processed_bookmaker_event_markets = [];
processed_events = []
processed_event_teams = []

events = {}

def init(title):
	initBookmakerEntities(title)
	initMappings(title)
	initEvents()

def initBookmakerEntities(title):
	global bookmaker_sports
	global bookmaker_tournaments
	global bookmaker_teams
	global bookmaker_markets
	global bookmaker_sports_to_skip
	global bookmaker_tournaments_to_skip
	global entities

	for entity in entities:
		csv_path = '../../cache/entities/' + title + '/' + entity + '.csv'
		if os.path.exists(csv_path):
			with open(csv_path, 'r', encoding="utf-8") as file:
				for line in file:
					row = line.strip().split('@s.s@')
					if entity == 'sports':
						# id@s.s@title@s.s@skip
						bookmaker_sports[row[1]] = {
							'id': row[0],
							'title': row[1]
						}

						if row[2] == '1':
							bookmaker_sports_to_skip.append(row[1])
					elif entity == 'tournaments':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip
						bookmaker_tournaments[row[3]] = {
							'id': row[2],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}

						if row[4] == '1':
							bookmaker_tournaments_to_skip.append(row[3])
					elif entity == 'teams':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip
						bookmaker_teams[row[3]] = {
							'id': row[2],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}
					elif entity == 'markets':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip
						bookmaker_markets[row[3]] = {
							'id': row[2],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}


def initMappings(title):
	global sports_maps
	global tournaments_maps
	global teams_maps
	global markets_maps
	global entities

	for entity in entities:
		csv_path = '../../cache/mappings/' + title + '/' + entity + '.csv'
		if os.path.exists(csv_path):
			with open(csv_path, 'r', encoding="utf-8") as file:
				for line in file:
					row = line.strip().split('@s.s@')
					if entity == 'sports':
						# entity_id@s.s@entity_title@s.s@entity_live_date_interval@s.s@bookmaker_entity_id@s.s@bookmaker_entity_title
						sports_maps[row[3]] = {
							'sport_id': row[0],
							'sport_title': row[1],
							'live_date_interval': row[2],
							'bookmaker_sport_id': row[3],
							'bookmaker_sport_title': row[4],
						}
					elif entity == 'tournaments':
						# entity_id@s.s@entity_title@s.s@entity_parent_id@s.s@entity_parent_title@s.s@bookmaker_entity_id@s.s@bookmaker_entity_title
						tournaments_maps[row[4]] = {
							'sport_id': row[2],
							'sport_title': row[3],
							'tournament_id': row[0],
							'tournament_title': row[1],
							'bookmaker_tournament_id': row[4],
							'bookmaker_tournament_title': row[5],
						}
					elif entity == 'teams':
						# entity_id@s.s@entity_title@s.s@entity_parent_id@s.s@entity_parent_title@s.s@bookmaker_entity_id@s.s@bookmaker_entity_title
						teams_maps[row[4]] = {
							'sport_id': row[2],
							'sport_title': row[3],
							'team_id': row[0],
							'team_title': row[1],
							'bookmaker_team_id': row[4],
							'bookmaker_team_title': row[5],
						}
					elif entity == 'markets':
						# entity_id@s.s@entity_title@s.s@entity_parent_id@s.s@entity_parent_title@s.s@bookmaker_entity_id@s.s@bookmaker_entity_title
						markets_maps[row[4]] = {
							'sport_id': row[2],
							'sport_title': row[3],
							'market_id': row[0],
							'market_title': row[1],
							'bookmaker_market_id': row[4],
							'bookmaker_market_title': row[5],
						}

def initEvents():
	global events

	csv_path = '../../cache/events.csv'
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				row = line.strip().split('@s.s@')
				print(row)
				event_id = row[0]
				event_date = row[1]
				event_time = row[2]
				event_title = row[3]
				tournament_id = row[4]
				event_team_id = row[5]
				# id@s.s@date@s.s@time@s.s@title@s.s@tournament_id@s.s@team_id
				if not tournament_id in events:
					events[tournament_id] = {}

				if not event_date in events[tournament_id]:
					events[tournament_id][event_date] = {}

				if not event_id in events[tournament_id][event_date]:
					events[tournament_id][event_date][event_id] = {
						'teams': [],
						'datetime': event_date + ' ' + event_time,
						'title': event_title
					}

				events[tournament_id][event_date][event_id]['teams'].append(event_team_id)

def shouldSkipSport(sport):
	return sport in bookmaker_sports_to_skip

def shouldSkipTournament(tournament):
	return tournament in bookmaker_tournaments_to_skip

def processEvent(bookmaker_event):
	global events_processed

	if (len(bookmaker_event.title) > 0 
		and len(bookmaker_event.sport) > 0 
		and len(bookmaker_event.tournament) > 0 
		and (len(bookmaker_event.date) > 0 or bookmaker_event.live)
		and not shouldSkipSport(bookmaker_event.sport)
		and not shouldSkipTournament(bookmaker_event.tournament)):

		#print('Processing: ' + bookmaker_event.title)

		buildBookmakerSport(bookmaker_event)
		buildBookmakerTournament(bookmaker_event)
		buildBookmakerTeams(bookmaker_event)
		buildBookmakerMarkets(bookmaker_event)
		buildEvent(bookmaker_event)
		events_processed += 1


def buildBookmakerSport(bookmaker_event):
	a = 1

def buildBookmakerTournament(bookmaker_event):
	a = 1

def buildBookmakerTeams(bookmaker_event):
	a = 1

def buildBookmakerMarkets(bookmaker_event):
	a = 1

def buildEvent(bookmaker_event):
	a = 1

def buildEventTeams(bookmaker_event, event_title, event_date):
	a = 1

def buildEventTeam(bookmaker_event, team, event_title, event_date, index):
	a = 1

def buildBookmakerEvent(bookmaker_event, event_title, event_date):
	a = 1

def buildBookmakerEventMarkets(bookmaker_event, event_title, event_date):
	a = 1

def getLiveDateBySport(live_date_interval = None, date = None):
	a = 1