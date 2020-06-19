import os
import csv
import time
import json
import re
import datetime
from datetime import date, timedelta
from slugify import slugify
import sys
sys.path.append("../../../")
from scripts import MarketParser
from models import MarketRule

# Constants
BOOKMAKER_SPORTS_TABLE = 'bookmaker_sports'
BOOKMAKER_SPORTS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'title', 'ignore', 'mapped', 'created_at']
BOOKMAKER_TOURNAMENTS_TABLE = 'bookmaker_tournaments'
BOOKMAKER_TOURNAMENTS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'fk_bookmaker_sport_id', 'title', 'skip', 'mapped', 'created_at', 'found_in']
BOOKMAKER_TEAMS_TABLE = 'bookmaker_teams'
BOOKMAKER_TEAMS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'fk_bookmaker_sport_id', 'title', 'found_in', 'ignore', 'mapped', 'created_at']
BOOKMAKER_MARKETS_TABLE = 'bookmaker_markets'
BOOKMAKER_MARKETS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'fk_bookmaker_sport_id', 'title', 'skip', 'outcomes', 'mapped', 'created_at']
EVENTS_TABLE = 'events'
EVENTS_TABLE_COLUMNS = ['id','fk_tournament_id', 'inserted_by', 'title', 'date', 'live_until', 'slug', 'live', 'top', 'active', 'has_markets', 'teams_count', 'time', 'has_members', 'created_at', 'related_to_market']
EVENT_TEAMS_TABLE = 'event_teams'
EVENT_TEAMS_TABLE_COLUMNS = ['id', 'fk_event_id', 'fk_team_id', 'local']
BOOKMAKER_EVENTS_TABLE = 'bookmaker_events'
BOOKMAKER_EVENTS_TABLE_COLUMNS = ['id', 'fk_bookmaker_id', 'fk_event_id', 'title', 'event_id', 'date']
BOOKMAKER_EVENT_MARKETS_TABLE = 'bookmaker_event_markets'
BOOKMAKER_EVENT_MARKETS_TABLE_COLUMNS = ['id', 'fk_bookmaker_event', 'fk_bookmaker_market', 'title', 'subtitle', 'market_id']
BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE = 'bookmaker_event_market_outcomes'
BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE_COLUMNS = ['id', 'fk_bookmaker_event_market_id', 'fk_team_id', 'outcome_id', 'title', 'decimal', 'old_decimal', 'created_at', 'updated_at', 'deep_link_json', 'outcome_rule_id']

EVENT_CHAMPIONSHIP_WINNER = 'Winner'
ACTIVE = 1
INACTIVE = 0
MYSQL_DATE_FORMAT = '%Y-%m-%d'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MYSQL_TIME_FORMAT = '%H:%M:%S'

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
processed_bookmaker_tournaments = {}
processed_bookmaker_teams = {}
processed_bookmaker_markets = {}
processed_bookmaker_event_markets = {}
processed_events = {}
processed_event_teams = {}

events = {}
outright_markets = {}

market_parser = None

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
	initMarketParser(title)
	initEvents()
	initNextSqlFile()

def finish():
	initNextSqlFile(False)

def initNextSqlFile(open_next = True):
	global processed_entities
	global processed_files

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
	processed_bookmaker_tournaments = {}
	processed_bookmaker_teams = {}
	processed_bookmaker_markets = {}
	processed_bookmaker_event_markets = {}
	processed_events = {}
	processed_event_teams = {}

def addProcessToQueue():
	global queue_path
	global queue_csv_path
	global timestamp
	global processed_files
	global bookmaker_title
	global bookmaker_id

	with open(queue_csv_path, 'a', encoding="utf-8") as fd:
		fd.write(timestamp + ";" + str(processed_files) + ";" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "\n")

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
						if row[1] not in bookmaker_tournaments:
							bookmaker_tournaments[row[1]] = {}

						bookmaker_tournaments[row[1]][row[3]] = {
							'id': row[2],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}

						if row[4] == '1':
							bookmaker_tournaments_to_skip.append(row[3])
					elif entity == 'teams':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip
						if row[1] not in bookmaker_teams:
							bookmaker_teams[row[1]] = {}

						bookmaker_teams[row[1]][row[3]] = {
							'id': row[2],
							'title': row[3],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}
					elif entity == 'markets':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip@s.s@outcomes
						if row[1] not in bookmaker_markets:
							bookmaker_markets[row[1]] = {}

						bookmaker_markets[row[1]][row[3]] = {
							'id': row[2],
							'title': row[3],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1],
							'outcomes': None if row[5] == '[]' or len(row[5]) == 0 else row[5]
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

def initMarketParser(title):
	global market_parser

	# Initialize
	market_parser = MarketParser.MarketParser()

	# Set constants
	csv_path = '../../cache/constants.csv'
	constants = {}
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				row = line.strip().split('@s.s@')
				variable = row[1]
				value = row[2]
				# id@s.s@variable@s.s@value
				variable = market_parser.CONSTANT.replace('{variable}', variable)
				constants[variable] = value

	market_parser.setConstantsVariables(constants)

	# Get market rules belonging to the current bookmaker
	csv_path = '../../cache/market_parser/markets/' + title + '/rules.csv'
	rules = []
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				row = line.strip().split('@s.s@')
				# sport_id@s.s@sport_title@s.s@market_id@s.s@market_title@s.s@input@s.s@input_replace@s.s@outcome_output
				rule = MarketRule.MarketRule()
				input_replace = []
				_input_replace = json.loads(row[5]) if row[5] != 'None' else []

				rule.sport_id = row[0]
				rule.sport_title = row[1]
				rule.market_id = row[2]
				rule.market_title = row[3]
				rule.input = row[4]
				rule.input_replace = input_replace
				rule.outcome_output = row[6]
				
				rules.append(rule)

	market_parser.setMarketsRules(rules)

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
	global processed_bookmaker_sports
	global processed_bookmaker_tournaments
	global bookmaker_tournaments
	global processed_entities

	if bookmaker_event.sport not in processed_bookmaker_tournaments:
		processed_bookmaker_tournaments[bookmaker_event.sport] = []

		if bookmaker_event.sport not in processed_bookmaker_sports:
			processed_bookmaker_sports.append(bookmaker_event.sport)

	if (
		 (bookmaker_event.sport not in bookmaker_tournaments or bookmaker_event.tournament not in bookmaker_tournaments[bookmaker_event.sport])
		 and bookmaker_event.tournament not in processed_bookmaker_tournaments[bookmaker_event.sport]
		):
		if processed_entities[BOOKMAKER_TOURNAMENTS_TABLE] == 0:
			sql = 'INSERT INTO ' + BOOKMAKER_TOURNAMENTS_TABLE + ' (' + ', '.join(BOOKMAKER_TOURNAMENTS_TABLE_COLUMNS) + ') VALUES \n'
		else:
			sql = '\n,'

		sql += "(DEFAULT, " + str(bookmaker_id) +", {sport=" + bookmaker_event.sport.replace("'", "´") + "}, '" + bookmaker_event.tournament.replace("'", "´") + "', " + str(INACTIVE) + ", " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "', '" + bookmaker_event.title.replace("'", "´")  + "')"
		with open(sql_files_path + BOOKMAKER_TOURNAMENTS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
			fd.write(sql)
		processed_bookmaker_tournaments[bookmaker_event.sport].append(bookmaker_event.tournament)
		processed_entities[BOOKMAKER_TOURNAMENTS_TABLE] += 1


def buildBookmakerTeams(bookmaker_event):
	global processed_bookmaker_sports
	global processed_bookmaker_teams
	global processed_entities
	global bookmaker_teams

	if bookmaker_event.sport not in processed_bookmaker_teams:
		processed_bookmaker_teams[bookmaker_event.sport] = []

		if bookmaker_event.sport not in processed_bookmaker_sports:
			processed_bookmaker_sports.append(bookmaker_event.sport)

	for team in bookmaker_event.teams:
		if (
				len(team.title)
				and (bookmaker_event.sport not in bookmaker_teams or team.title not in bookmaker_teams[bookmaker_event.sport])
				and team.title not in processed_bookmaker_teams[bookmaker_event.sport]
			):
			if processed_entities[BOOKMAKER_TEAMS_TABLE] == 0:
				sql = 'INSERT INTO ' + BOOKMAKER_TEAMS_TABLE + ' (' + ', '.join(BOOKMAKER_TEAMS_TABLE_COLUMNS) + ') VALUES \n'
			else:
				sql = '\n,'

			sql += "(DEFAULT, " + str(bookmaker_id) + ", {sport=" + bookmaker_event.sport.replace("'", "´") + "}, '" + team.title.replace("'", "´") + "', '" + bookmaker_event.title.replace("'", "´") + "', " + str(INACTIVE) + ", " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "')"
			with open(sql_files_path + BOOKMAKER_TEAMS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
				fd.write(sql)
			processed_bookmaker_teams[bookmaker_event.sport].append(team.title)
			processed_entities[BOOKMAKER_TEAMS_TABLE] += 1

		if len(team.members):
			for member in team.members:
				if (
						len(member.title)
						and (bookmaker_event.sport not in bookmaker_teams or member.title not in bookmaker_teams[bookmaker_event.sport])
						and member.title not in processed_bookmaker_teams[bookmaker_event.sport]
					):
					if processed_entities[BOOKMAKER_TEAMS_TABLE] == 0:
						sql = 'INSERT INTO ' + BOOKMAKER_TEAMS_TABLE + ' (' + ', '.join(BOOKMAKER_TEAMS_TABLE_COLUMNS) + ') VALUES \n'
					else:
						sql = '\n,'

					sql += "(DEFAULT, " + str(bookmaker_id) + ", {sport=" + bookmaker_event.sport.replace("'", "´") + "}, '" + member.title.replace("'", "´") + "', '" + bookmaker_event.title.replace("'", "´") + "', " + str(INACTIVE) + ", " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "')"
					with open(sql_files_path + BOOKMAKER_TEAMS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
						fd.write(sql)
					processed_bookmaker_teams[bookmaker_event.sport].append(team.title)
					processed_entities[BOOKMAKER_TEAMS_TABLE] += 1

def buildBookmakerMarkets(bookmaker_event):
	global processed_bookmaker_sports
	global processed_bookmaker_markets
	global bookmaker_sports
	global sports_maps
	global processed_entities
	global bookmaker_markets
	global markets_maps
	global bookmaker_tournaments
	global tournaments_maps
	global market_parser

	if bookmaker_event.sport not in processed_bookmaker_markets:
		processed_bookmaker_markets[bookmaker_event.sport] = []

		if bookmaker_event.sport not in processed_bookmaker_sports:
			processed_bookmaker_sports.append(bookmaker_event.sport)

	for odd in bookmaker_event.odds:
		outcomes_titles = []

		# Bookmaker event market
		if (
				bookmaker_event.sport in bookmaker_markets
				and odd.title in bookmaker_markets[bookmaker_event.sport]
				and bookmaker_event.tournament in bookmaker_tournaments[bookmaker_event.sport]
				and bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]['id'] in tournaments_maps
			):
			if bookmaker_markets[bookmaker_event.sport][odd.title]['id'] in markets_maps:
				bookmaker_event.has_markets = True
				odd.is_mapped = True

			# Check market rules
			market_rule = market_parser.getMarketRule(odd.title, sports_maps[bookmaker_sports[bookmaker_event.sport]['id']]['sport_title'])

			if odd.title in bookmaker_markets[bookmaker_event.sport] and market_rule:
				bookmaker_market = bookmaker_markets[bookmaker_event.sport][odd.title]
				market_map = {
					'sport_id': market_rule.sport_id,
					'sport_title': market_rule.sport_title,
					'market_id': market_rule.market_id,
					'market_title': market_rule.market_title,
					'bookmaker_market_id': bookmaker_market['id'],
					'bookmaker_market_title': bookmaker_market['title']
				}

				markets_maps[bookmaker_market['id']] = market_map

				odd.outcomes = market_parser.filterMarketOutcomes(market_rule, odd.title, odd.outcomes, bookmaker_event.teams)
				odd.is_mapped = True
				odd.has_markets = True


		# Bookmaker market
		if (
				len(odd.title) > 0
				and (bookmaker_event.sport not in bookmaker_markets or odd.title not in bookmaker_markets[bookmaker_event.sport] or len(bookmaker_markets[bookmaker_event.sport][odd.title]['outcomes']) == 0)
				and odd.title not in processed_bookmaker_markets[bookmaker_event.sport]
			):
			if processed_entities[BOOKMAKER_MARKETS_TABLE] == 0:
				sql = 'INSERT INTO ' + BOOKMAKER_MARKETS_TABLE + ' (' + ', '.join(BOOKMAKER_MARKETS_TABLE_COLUMNS) + ') VALUES \n'
			else:
				sql = '\n,'

			for outcome in odd.outcomes:
				outcomes_titles.append(outcome.title.replace("'", "´"))

			sql += "(DEFAULT, " + str(bookmaker_id) + ", {sport=" + bookmaker_event.sport.replace("'", "´") + "}, '" + odd.title.replace("'", '´') + "', " + str(INACTIVE) + ", '" + json.dumps(outcomes_titles) + "', " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "')"
			with open(sql_files_path + BOOKMAKER_MARKETS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
				fd.write(sql)
			processed_bookmaker_markets[bookmaker_event.sport].append(odd.title)
			processed_entities[BOOKMAKER_MARKETS_TABLE] += 1

def buildEvent(bookmaker_event):
	global bookmaker_sports
	global bookmaker_tournaments
	global bookmaker_teams
	global sports_maps
	global tournaments_maps
	global teams_maps
	global processed_events
	global events
	global outright_markets
	global processed_entities
	global sql_files_path

	if bookmaker_event.has_markets:
		now = datetime.datetime.now()
		today = now.strftime(MYSQL_DATE_FORMAT)
		today_full = now.strftime(MYSQL_DATETIME_FORMAT)
		bookmaker_sport = bookmaker_sports[bookmaker_event.sport]
		bookmaker_tournament = bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]
		sport_id = sports_maps[bookmaker_sport['id']]['sport_id']
		sport_title = sports_maps[bookmaker_sport['id']]['sport_title']
		sport_live_date_interval = sports_maps[bookmaker_sport['id']]['live_date_interval']

		if bookmaker_tournament['id'] in tournaments_maps:
			tournament = tournaments_maps[bookmaker_tournament['id']]
			tournament_id = tournament['tournament_id']
			tournament_title = tournament['tournament_title']

			if sport_title not in processed_events:
				processed_events[sport_title] = {}

			if tournament_title not in processed_events[sport_title]:
				processed_events[sport_title][tournament_title] = {}

			# Get teams and build event name
			event_between_two_teams = False
			event_between_two_teams_with_members = False
			has_members = False
			event_name = bookmaker_event.title
			teams_count = 0
			teams_names = []
			teams_ids = []

			if len(bookmaker_event.teams) > 0 and bookmaker_event.replace_title != EVENT_CHAMPIONSHIP_WINNER:
				for team in bookmaker_event.teams:
					if len(team.members) > 0:
						members_names = []
						has_members = True
						for member in team.members:
							if (
								bookmaker_event.sport in bookmaker_teams
								and member.title in bookmaker_teams[bookmaker_event.sport]
								and bookmaker_teams[bookmaker_event.sport][member.title]['id'] in teams_maps
							):
								teams_ids.append(teams_maps[bookmaker_teams[bookmaker_event.sport][member.title]['id']]['team_id'])
								members_names.append(teams_maps[bookmaker_teams[bookmaker_event.sport][member.title]['id']]['team_title'])

						if len(members_names) == 2:
							teams_names.append(members_names[0] + ' / ' + members_names[1])
							teams_count += 1
					elif (
						bookmaker_event.sport in bookmaker_teams
						and team.title in bookmaker_teams[bookmaker_event.sport]
						and bookmaker_teams[bookmaker_event.sport][team.title]['id'] in teams_maps
					):
						teams_ids.append(teams_maps[bookmaker_teams[bookmaker_event.sport][team.title]['id']]['team_id'])
						teams_names.append(teams_maps[bookmaker_teams[bookmaker_event.sport][team.title]['id']]['team_title'])
						teams_count += 1

				if has_members and len(teams_names) == 2 and len(bookmaker_event.teams) == 2:
					event_between_two_teams_with_members = True
					event_name = teams_names[0] + ' vs ' + teams_names[1]
				elif not has_members and len(teams_names) == 2 and len(bookmaker_event.teams) == 2:
					event_between_two_teams = True
					event_name = teams_names[0] + ' vs ' + teams_names[1]

			# If it's a live event and has no date, look for the matching event today
			if len(bookmaker_event.date) == 0 and bookmaker_event.live and tournament_id in events and today in events[tournament_id]:
				for event_id in events[tournament_id][today]:
					event = events[tournament_id][date_part][event_id]
					ids = event['teams']
					if (
						len(teams_ids) == 2 
						and len(ids) == 2 
						and (teams_ids[0] == ids[0] or teams_ids[0] == ids[1])
						and (teams_ids[1] == ids[0] or teams_ids[1] == ids[1])
					):
						bookmaker_event.date = event['datetime']
						break

			if (
				len(bookmaker_event.date) > 0
				and len(event_name) > 0
				and (
					(not has_members and not event_between_two_teams and ((teams_count > 0 and len(bookmaker_event.teams) != 2) or bookmaker_event.replace_title == EVENT_CHAMPIONSHIP_WINNER)) # Outright
					or (not has_members and event_between_two_teams and teams_count == 2) # 1 vs 1
					or (has_members and event_between_two_teams_with_members and teams_count == 2) # 2 vs 2
				)
			):
				date = datetime.datetime.strptime(bookmaker_event.date, MYSQL_DATETIME_FORMAT)
				event_date = date.strftime(MYSQL_DATETIME_FORMAT)
				related_market_id = None
				insert_event = True

				# Check if this event can be inserted
				if len(bookmaker_event.replace_title) > 0 and bookmaker_event.replace_title == EVENT_CHAMPIONSHIP_WINNER and sport_id in outright_markets:
					related_market_id = outright_markets[sport_id]
				else:
					# Check current, previous and next dates
					two_days_ago_date = (date - timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)
					previous_date = (date - timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
					next_date = (date + timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
					two_days_after_date = (date + timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)

					dates_to_check = [today, previous_date, next_date, two_days_after_date, two_days_ago_date]

					for date_part in dates_to_check:
						# Check if this order of teams is the opposite of an existing event on DB
						if tournament_id in events and date_part in events[tournament_id]:
							for event_id in events[tournament_id][date_part]:
								event = events[tournament_id][date_part][event_id]

								ids = event['teams']
								if (
                                    len(teams_ids) == 2
                                    and len(ids) == 2
                                    and (teams_ids[0] == ids[0] or teams_ids[0] == ids[1])
                                    and (teams_ids[1] == ids[0] or teams_ids[1] == ids[1])
                                ):
									existing_event_name = event['title']

									if existing_event_name != event_name:
										event_name = existing_event_name;

									event_date = event['datetime']
									insert_event = False
									break

                    # Replace parameters
					event_name = event_name.replace("'", "´")

				if insert_event and not bookmaker_event.live and event_name not in processed_events[sport_title][tournament_title]:
					date = datetime.datetime.strptime(event_date, MYSQL_DATETIME_FORMAT)
					event_date = date.strftime(MYSQL_DATETIME_FORMAT)
					live_date = getLiveDateBySport(sport_live_date_interval, event_date)
					timestamp = int(datetime.datetime.timestamp(date))
					slug = slugify(event_name) + '-' + str(timestamp)

					if processed_entities[EVENTS_TABLE] == 0:
						sql = "INSERT INTO " + EVENTS_TABLE + " (" + ', '.join(EVENTS_TABLE_COLUMNS) + ") VALUES \n";
					else:
						sql = "\n,"

					sql += "(DEFAULT, " + str(tournament_id) + ", " + str(bookmaker_id) + ", '{event_title}', '" + date.strftime(MYSQL_DATE_FORMAT) + "', {live_date}, '{event_slug}', " + str(INACTIVE) + ", " + str(INACTIVE) + ", " + str(ACTIVE) + ", " + (str(ACTIVE) if bookmaker_event.has_markets else str(INACTIVE)) + ", {teams_count}, '" + date.strftime(MYSQL_TIME_FORMAT) + "', " + (str(ACTIVE) if event_between_two_teams_with_members else str(INACTIVE)) + ", '" + today_full + "', " + (related_market_id if related_market_id else 'NULL') + "){teams=" + ",".join(teams_ids) + "}"
					sql = sql.replace('{event_title}', event_name)
					sql = sql.replace('{event_slug}', slug)
					sql = sql.replace('{teams_count}', str(teams_count))
					sql = sql.replace('{live_date}', "'" + live_date + "'" if live_date else 'NULL')

					with open(sql_files_path + EVENTS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
						fd.write(sql)

					processed_events[sport_title][tournament_title][event_name] = {
						'date': event_date,
						'title': event_name,
						'tournament_id': tournament_id,
						'tournament_title': tournament_title,
						'sport_title': sport_title
					}

					buildEventTeams(bookmaker_event, event_name, event_date)
					buildBookmakerEvent(bookmaker_event, event_name, event_date)
					processed_entities[EVENTS_TABLE] += 1
				elif event_name not in processed_events[sport_title][tournament_title]:
					processed_events[sport_title][tournament_title][event_name] = {
						'date': event_date,
						'title': event_name,
						'tournament_id': tournament_id,
						'tournament_title': tournament_title,
						'sport_title': sport_title
					}

					buildEventTeams(bookmaker_event, event_name, event_date)
					buildBookmakerEvent(bookmaker_event, event_name, event_date)
				else:
					buildBookmakerEventMarkets(bookmaker_event, event_name, event_date);

def buildEventTeams(bookmaker_event, event_title, event_date):
	global bookmaker_sports
	global sports_maps
	global bookmaker_tournaments
	global tournaments_maps
	global processed_event_teams

	bookmaker_sport = bookmaker_sports[bookmaker_event.sport]
	bookmaker_tournament = bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]
	sport_title = sports_maps[bookmaker_sport['id']]['sport_title']
	tournament = tournaments_maps[bookmaker_tournament['id']]
	tournament_title = tournament['tournament_title']

	if sport_title not in processed_event_teams:
		processed_event_teams[sport_title] = {}

	if tournament_title not in processed_event_teams[sport_title]:
		processed_event_teams[sport_title][tournament_title] = {}

	if event_title not in processed_event_teams[sport_title][tournament_title]:
		processed_event_teams[sport_title][tournament_title][event_title] = []

	index = 0
	for team in bookmaker_event.teams:
		if len(team.members) > 0:
			for member in team.members:
				buildEventTeam(bookmaker_event, member, event_title, event_date, index)
		else:
			buildEventTeam(bookmaker_event, team, event_title, event_date, index)

		index += 1

def buildEventTeam(bookmaker_event, team, event_title, event_date, index):
	global bookmaker_sports
	global sports_maps
	global bookmaker_tournaments
	global tournaments_maps
	global bookmaker_teams
	global teams_maps
	global processed_event_teams
	global processed_entities

	bookmaker_sport = bookmaker_sports[bookmaker_event.sport]
	bookmaker_tournament = bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]
	sport_title = sports_maps[bookmaker_sport['id']]['sport_title']
	tournament = tournaments_maps[bookmaker_tournament['id']]
	tournament_title = tournament['tournament_title']

	if (
		team.title not in processed_event_teams[sport_title][tournament_title][event_title]
		and bookmaker_event.sport in bookmaker_teams
		and team.title in bookmaker_teams[bookmaker_event.sport]
		and bookmaker_teams[bookmaker_event.sport][team.title]['id'] in teams_maps
	):
		if processed_entities[EVENT_TEAMS_TABLE] == 0:
			sql = "INSERT INTO " + EVENT_TEAMS_TABLE + " (" + ', '.join(EVENT_TEAMS_TABLE_COLUMNS) + ") values \n";
		else:
			sql = "\n,"

		sql += "(DEFAULT, {sport=" + sport_title + "&tournament=" + tournament_title + "&event=" + event_title + "&date=" + event_date + "}, " + teams_maps[bookmaker_teams[bookmaker_event.sport][team.title]['id']]['team_id'] + ", " + (str(ACTIVE) if team.local and index == 0 else str(INACTIVE)) + ")"

		with open(sql_files_path + EVENT_TEAMS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
			fd.write(sql)

		processed_event_teams[sport_title][tournament_title][event_title].append(team.title)
		processed_entities[EVENT_TEAMS_TABLE] += 1


def buildBookmakerEvent(bookmaker_event, event_title, event_date):
	global bookmaker_id
	global bookmaker_sports
	global sports_maps
	global bookmaker_tournaments
	global tournaments_maps
	global processed_entities

	bookmaker_sport = bookmaker_sports[bookmaker_event.sport]
	bookmaker_tournament = bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]
	sport_title = sports_maps[bookmaker_sport['id']]['sport_title']
	tournament = tournaments_maps[bookmaker_tournament['id']]
	tournament_title = tournament['tournament_title']

	if processed_entities[BOOKMAKER_EVENTS_TABLE] == 0:
		sql = "INSERT INTO " + BOOKMAKER_EVENTS_TABLE + " (" + ', '.join(BOOKMAKER_EVENTS_TABLE_COLUMNS) + ") values \n";
	else:
		sql = "\n,"

	sql += "(DEFAULT, " + str(bookmaker_id) + ", {sport=" + sport_title + "&tournament=" + tournament_title + "&event=" + event_title + "&date=" + event_date + "}, '" + event_title + "', " + ("'" + bookmaker_event.event_id + "'" if len(bookmaker_event.event_id) > 0 else "NULL") + ", " + ("'" + bookmaker_event.date + "'" if len(bookmaker_event.date) else "NULL") + ")"

	with open(sql_files_path + BOOKMAKER_EVENTS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
		fd.write(sql)

	processed_entities[BOOKMAKER_EVENTS_TABLE] += 1
	buildBookmakerEventMarkets(bookmaker_event, event_title, event_date)

def buildBookmakerEventMarkets(bookmaker_event, event_title, event_date):
	global bookmaker_id
	global bookmaker_sports
	global sports_maps
	global bookmaker_tournaments
	global tournaments_maps
	global processed_entities
	global processed_bookmaker_event_markets
	global bookmaker_markets
	global markets_maps

	bookmaker_sport = bookmaker_sports[bookmaker_event.sport]
	bookmaker_tournament = bookmaker_tournaments[bookmaker_event.sport][bookmaker_event.tournament]
	sport_title = sports_maps[bookmaker_sport['id']]['sport_title']
	tournament = tournaments_maps[bookmaker_tournament['id']]
	tournament_title = tournament['tournament_title']

	for odd in bookmaker_event.odds:
		if odd.is_mapped:
			if not sport_title in processed_bookmaker_event_markets:
				processed_bookmaker_event_markets[sport_title] = {}

			if not tournament_title in processed_bookmaker_event_markets[sport_title]:
				processed_bookmaker_event_markets[sport_title][tournament_title] = {}

			if not event_title in processed_bookmaker_event_markets[sport_title][tournament_title]:
				processed_bookmaker_event_markets[sport_title][tournament_title][event_title] = []

			# Check whether this market has been already processed for this event or not
			bookmaker_market_id = bookmaker_markets[bookmaker_event.sport][odd.title]['id']
			market = markets_maps[bookmaker_markets[bookmaker_event.sport][odd.title]['id']]
			market_title = market['market_title']

			if not market_title in processed_bookmaker_event_markets[sport_title][tournament_title][event_title]:
				# Bookmaker event market
				if processed_entities[BOOKMAKER_EVENT_MARKETS_TABLE] == 0:
					sql = "INSERT INTO " + BOOKMAKER_EVENT_MARKETS_TABLE + " (" + ", ".join(BOOKMAKER_EVENT_MARKETS_TABLE_COLUMNS) + ") VALUES \n"
				else:
					sql = "\n,"

				sql += "(DEFAULT, {sport=" + sport_title + "&tournament=" + tournament_title + "&event=" + event_title + "&date=" + event_date + "}, " + str(bookmaker_market_id) + ", '" + market_title + "', '', '" + odd.id + "')"

				with open(sql_files_path + BOOKMAKER_EVENT_MARKETS_TABLE + '.sql', 'a', encoding="utf-8") as fd:
					fd.write(sql)

				processed_entities[BOOKMAKER_EVENT_MARKETS_TABLE] += 1
				processed_bookmaker_event_markets[sport_title][tournament_title][event_title].append(market_title)

				# Outcomes
				for outcome in odd.outcomes:
					if outcome.decimal > 1:
						# Bookmaker event market outcome
						if processed_entities[BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE] == 0:
							sql = "INSERT INTO " + BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE + " (" + ", ".join(BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE_COLUMNS) + ") VALUES \n"
						else:
							sql = "\n,"

						teams_titles = []

						for team in bookmaker_event.teams:
							teams_titles.append(team.title)

						sql += "(DEFAULT, {sport=" + bookmaker_event.sport + "&tournament=" + bookmaker_event.tournament + "&event=" + event_title + "&date=" + event_date + "&market_title=" + odd.title + "&teams=" + json.dumps(teams_titles) + "}, {team=" + outcome.title.strip() + "}, '" + outcome.outcome_id + "', {outcome_title=" + outcome.title.strip() + "}, " + str(round(outcome.decimal, 2)) + ", NULL, '{created_at}', '{updated_at}', " + ("'" + json_encode(outcome.deep_link) + "'" if outcome.deep_link else 'NULL') + ", {outcome_rule_id})"

						with open(sql_files_path + BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE + '.sql', 'a', encoding="utf-8") as fd:
							fd.write(sql)

						processed_entities[BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE] += 1


def getLiveDateBySport(live_date_interval = None, date = None):
	output = None

	try:
		date = datetime.datetime.strptime(date, MYSQL_DATETIME_FORMAT)

		if date and live_date_interval:
			m = re.search('PT(\d+)H(\d*)[M]?', live_date_interval)
			hours = int(m.group(1).strip())
			minutes = m.group(2).strip()

			if len(minutes) == 0:
				minutes = 0
			else:
				minutes = int(minutes)

			date = date + timedelta(hours=hours, minutes=minutes)
			output = date.strftime(MYSQL_DATETIME_FORMAT)
	except (Exception) as ex:
		output = None

	return output