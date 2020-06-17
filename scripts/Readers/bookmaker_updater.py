import os
import csv
import time
from datetime import date

# Constants
BOOKMAKER_SPORTS_TABLE = 'bookmaker_sports'
BOOKMAKER_SPORTS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'title', 'ignore', 'mapped', 'created_at']
BOOKMAKER_TOURNAMENTS_TABLE = 'bookmaker_tournaments'
BOOKMAKER_TEAMS_TABLE = 'bookmaker_teams'
BOOKMAKER_MARKETS_TABLE = 'bookmaker_markets'
EVENTS_TABLE = 'events'
EVENT_TEAMS_TABLE = 'event_teams'
BOOKMAKER_EVENTS_TABLE = 'bookmaker_events'
BOOKMAKER_EVENT_MARKETS_TABLE = 'bookmaker_event_markets'
BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE = 'bookmaker_event_market_outcomes'

ACTIVE = 1
INACTIVE = 0
MYSQL_DATE_FORMAT = '%d-%m-%Y'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Variables
bookmaker_id = None
bookmaker_title = None

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
processed_entities = {
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

queue_path = None
queue_csv_path = None
sql_files_path = None
timestamp = None

processed_bookmaker_sports = []
processed_bookmaker_tournaments = []
processed_bookmaker_teams = []
processed_bookmaker_markets = []
processed_bookmaker_event_markets = [];
processed_events = []
processed_event_teams = []

events = {}

def init(id, title):
	global bookmaker_id
	global bookmaker_title
	global processed_files
	global queue_path
	global queue_csv_path
	global timestamp

	bookmaker_id = id
	bookmaker_title = title
	timestamp = str(int(time.time()))
	queue_path = '../../../queues/Readers/' + bookmaker_title + '/' + timestamp + '/'
	queue_csv_path = '../../../queues/Readers/' + bookmaker_title + '/queue.csv'

	if not os.path.exists(queue_path):
		os.makedirs(queue_path)

	initBookmakerEntities(title)
	initMappings(title)
	initEvents()
	initNextSqlFile()

def finish():
	initNextSqlFile(False)

def initNextSqlFile(open_next = True):
	global processed_entities

	# Check if there is any entity left
	add_to_queue = False
	for entity in processed_entities:
		if processed_entities[entity] > 0:
			add_to_queue = True
			break

	closeSqlFiles()

	if add_to_queue:
		addProcessToQueue()
		processed_files += 1

	resetCounters()
	if open_next:
		setSqlFilesPath()

def setSqlFilesPath():
	global queue_path
	global processed_files
	global sql_files_path

	sql_files_path = queue_path + str(processed_files) + '/'

	if not os.path.exists(sql_files_path):
		os.makedirs(sql_files_path)

def closeSqlFiles():
	global processed_entities
	global sql_files_path

	if processed_entities[BOOKMAKER_SPORTS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_SPORTS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_id, title) DO NOTHING;")

	if processed_entities[BOOKMAKER_TOURNAMENTS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_TOURNAMENTS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;")

	if processed_entities[BOOKMAKER_TEAMS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_TEAMS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;")

	if processed_entities[BOOKMAKER_MARKETS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_MARKETS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO UPDATE SET outcomes = EXCLUDED.outcomes;")

	if processed_entities[BOOKMAKER_EVENTS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_EVENTS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_id, fk_event_id) DO NOTHING;")

	if processed_entities[BOOKMAKER_EVENT_MARKETS_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_EVENT_MARKETS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_event, fk_bookmaker_market) DO UPDATE SET title = EXCLUDED.title;")

	if processed_entities[BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE] > 0:
		path = sql_files_path + BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_bookmaker_event_market_id, title) DO UPDATE SET old_decimal = " + BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE + ".decimal, decimal = EXCLUDED.decimal, deep_link_json = EXCLUDED.deep_link_json, updated_at = EXCLUDED.updated_at, outcome_rule_id = EXCLUDED.outcome_rule_id;")

	if processed_entities[EVENTS_TABLE] > 0:
		path = sql_files_path + EVENTS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_tournament_id, title, date) DO UPDATE SET time = EXCLUDED.time, has_markets = EXCLUDED.has_markets;")

	if processed_entities[EVENT_TEAMS_TABLE] > 0:
		path = sql_files_path + EVENT_TEAMS_TABLE + '.sql'
		with open(path, 'a', encoding="utf-8") as fd:
			fd.write("\nON CONFLICT (fk_event_id, fk_team_id) DO NOTHING;")

def resetCounters():
	global processed_entities
	global processed_bookmaker_sports
	global processed_bookmaker_tournaments
	global processed_bookmaker_teams
	global processed_bookmaker_markets
	global processed_bookmaker_event_markets
	global processed_events
	global processed_event_teams

	processed_entities = {
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
	processed_bookmaker_event_markets = []
	processed_events = []
	processed_event_teams = []

def addProcessToQueue():
	global queue_path
	global queue_csv_path
	global timestamp
	global processed_files
	global bookmaker_title
	global bookmaker_id

	with open(queue_csv_path, 'a', encoding="utf-8") as fd:
		fd.write(timestamp + ";" + str(processed_files) + ";" + date.today().strftime(MYSQL_DATETIME_FORMAT))

def initBookmakerEntities(title):
	global bookmaker_sports
	global bookmaker_tournaments
	global bookmaker_teams
	global bookmaker_markets
	global bookmaker_sports_to_skip
	global bookmaker_tournaments_to_skip
	global entities

	bookmaker_sports = {}
	bookmaker_tournaments = {}
	bookmaker_teams = {}
	bookmaker_markets = {}
	bookmaker_sports_to_skip = []
	bookmaker_tournaments_to_skip = []

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

	sports_maps = {}
	tournaments_maps = {}
	teams_maps = {}
	markets_maps = {}

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
	global processed_entities

	if (len(bookmaker_event.title) > 0 
		and len(bookmaker_event.sport) > 0 
		and len(bookmaker_event.tournament) > 0 
		and (len(bookmaker_event.date) > 0 or bookmaker_event.live)
		and not shouldSkipSport(bookmaker_event.sport)
		and not shouldSkipTournament(bookmaker_event.tournament)):

		buildBookmakerSport(bookmaker_event)
		buildBookmakerTournament(bookmaker_event)
		buildBookmakerTeams(bookmaker_event)
		buildBookmakerMarkets(bookmaker_event)
		buildEvent(bookmaker_event)
		events_processed += 1

		if (
			(processed_entities[EVENTS_TABLE] > 0 and processed_entities[EVENTS_TABLE] % 100 == 0)
			or (processed_entities[BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE] > 0 and processed_entities[BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE] >= 2500)
			):
			initNextSqlFile()


def buildBookmakerSport(bookmaker_event):
	global bookmaker_sports
	global processed_bookmaker_sports
	global processed_entities
	global bookmaker_id
	global sql_files_path

	if not bookmaker_event.sport in bookmaker_sports and bookmaker_event.sport not in processed_bookmaker_sports:
		if processed_entities[BOOKMAKER_SPORTS_TABLE] == 0:
			sql = 'INSERT INTO ' + BOOKMAKER_SPORTS_TABLE + ' (' + ', '.join(BOOKMAKER_SPORTS_TABLE_COLUMNS) + ') VALUES \n'
		else:
			sql = '\n,'

		sql += "(DEFAULT, " + str(bookmaker_id) + ", '" + bookmaker_event.sport + "', " + str(INACTIVE) + ", " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "')"
		with open(sql_files_path + BOOKMAKER_SPORTS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
			fd.write(sql)
		processed_bookmaker_sports.append(bookmaker_event.sport)
		processed_entities[BOOKMAKER_SPORTS_TABLE] += 1
		

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