import bookmaker_consumer
import sys
import re
import psycopg2
import os
import csv
from datetime import date, timedelta
sys.path.append("../")
sys.path.append("../../")
from scripts import MarketParser
from models import MarketRule, OutcomeRule

# Constants
NOT_PROCESSED_STRUCTURE = {'mapped': {}, 'not_mapped': {}, 'error': {}}
MYSQL_DATE_FORMAT = '%Y-%m-%d'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

entities = ['sports', 'tournaments', 'teams', 'markets']

bookmaker_sports = {}
bookmaker_tournaments = {}
bookmaker_teams = {}
bookmaker_markets = {}

sports_maps = {}
tournaments_maps = {}
teams_maps = {}
markets_maps = {}

not_processed_outcomes = NOT_PROCESSED_STRUCTURE

saved_outcomes = {}
queue_path = None
queue_csv_path = None

market_parser = None

bookmaker_id = None
bookmaker_title = None

connection = None

events = {}

def run(id, title):
	global bookmaker_id
	global bookmaker_title
	global queue_path
	global connection
	global cursor

	if connection == None:
		connection = psycopg2.connect(user = "asanchez",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet")

	bookmaker_id = id
	bookmaker_title = title
	queue_csv_path = '../../queues/Readers/' + bookmaker_title + '/queue.csv'

	# Init bookmaker entities
	initBookmakerEntities(title)
	initMappings(title)
	initMarketParser(title)
	initEvents()

	if os.path.exists(queue_csv_path):
		with open(queue_csv_path, 'r') as file:
			reader = csv.reader(file, delimiter=';')
			for row in reader:
				timestamp = row[0]
				page = row[1]
				queue_path = '../../queues/Readers/' + bookmaker_title + '/' + timestamp + '/' + page + '/'

				if os.path.exists(queue_path):
					seedDB()

					# Remove folder
					#os.remove(queue_path)

def initBookmakerEntities(title):
	global bookmaker_sports
	global bookmaker_tournaments
	global bookmaker_teams
	global bookmaker_markets
	global entities

	bookmaker_sports = {}
	bookmaker_tournaments = {}
	bookmaker_teams = {}
	bookmaker_markets = {}

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
					elif entity == 'tournaments':
						# parent_id@s.s@parent_title@s.s@id@s.s@title@s.s@skip
						if row[1] not in bookmaker_tournaments:
							bookmaker_tournaments[row[1]] = {}

						bookmaker_tournaments[row[1]][row[3]] = {
							'id': row[2],
							'bookmaker_sport_id': row[0],
							'bookmaker_sport_title': row[1]
						}
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
	global teams_maps

	# Initialize
	market_parser = MarketParser.MarketParser()

	# Set teams map
	market_parser.setTeamsMap(teams_maps)

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

	# Get outcome rules belonging to the current bookmaker
	csv_path = '../../cache/market_parser/outcomes/' + title + '/rules.csv'
	rules = []
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				row = line.strip().split('@s.s@')
				# market_id@s.s@market_title@s.s@sport_id@s.s@sport_title@s.s@id@s.s@input@s.s@input_replace@s.s@outcome_name@s.s@outcome_save_patter@s.s@outcome_output
				rule = OutcomeRule.OutcomeRule()
				input_replace = []
				_input_replace = json.loads(row[3]) if row[3] != 'None' else []

				rule.id = row[4]
				rule.sport_title = row[2]
				rule.market_title = row[1]
				rule.input = row[5]
				rule.input_replace = _input_replace
				rule.output = row[8]

				rules.append(rule)

	market_parser.setOutcomesRules(rules)

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

def seedDB():
	# Seed
	seedBookmakerSports()
	seedBookmakerTournaments()
	seedBookmakerTeams()
	seedBookmakerMarkets()
	seedEvents()

def seedBookmakerSports():
	global queue_path
	global connection

	sql_path = queue_path + 'bookmaker_sports.sql'
	if os.path.exists(sql_path):
		try:
			file = open(sql_path, 'r', encoding="utf-8")
			sql = file.read()
			cursor = connection.cursor()
			cursor.execute(sql)
			connection.commit()
			print('Bookmaker sports inserted successfully')
		except (Exception, psycopg2.DatabaseError) as error:
			print('Could not insert bookmaker sports: ' + str(error))
			connection.rollback()

def seedBookmakerTournaments():
	global queue_path
	global connection
	global bookmaker_sports

	sql_path = queue_path + 'bookmaker_tournaments.sql'
	if os.path.exists(sql_path):
		try:
			file = open(sql_path, 'r', encoding="utf-8")
			sql = file.read()
			matches = re.findall('{sport=(.*)}', sql)
			
			for match in matches:
				match = match.strip()
				if match in bookmaker_sports:
					sql = sql.replace('{sport=' + match + '}', bookmaker_sports[match]['id'])

			cursor = connection.cursor()
			cursor.execute(sql)
			connection.commit()
		except (Exception, psycopg2.DatabaseError) as error:
			print('Could not insert bookmaker tournaments: ' + str(error))
			connection.rollback()

def seedBookmakerTeams():
	global queue_path
	global connection
	global bookmaker_sports

	sql_path = queue_path + 'bookmaker_teams.sql'
	if os.path.exists(sql_path):
		try:
			file = open(sql_path, 'r', encoding="utf-8")
			sql = file.read()
			matches = re.findall('{sport=(.*)}', sql)
			
			for match in matches:
				match = match.strip()
				if match in bookmaker_sports:
					sql = sql.replace('{sport=' + match + '}', bookmaker_sports[match]['id'])

			cursor = connection.cursor()
			cursor.execute(sql)
			connection.commit()
		except (Exception, psycopg2.DatabaseError) as error:
			print('Could not insert bookmaker teams: ' + str(error))
			connection.rollback()

def seedBookmakerMarkets():
	global queue_path
	global connection
	global bookmaker_sports

	sql_path = queue_path + 'bookmaker_markets.sql'
	if os.path.exists(sql_path):
		try:
			file = open(sql_path, 'r', encoding="utf-8")
			sql = file.read()
			matches = re.findall('{sport=(.*)}', sql)
			
			for match in matches:
				match = match.strip()
				if match in bookmaker_sports:
					sql = sql.replace('{sport=' + match + '}', bookmaker_sports[match]['id'])

			cursor = connection.cursor()
			cursor.execute(sql)
			connection.commit()
		except (Exception, psycopg2.DatabaseError) as error:
			print('Could not insert bookmaker markets: ' + str(error))
			connection.rollback()

def seedEvents():
	global queue_path
	global connection
	global bookmaker_sports

	sql_path = queue_path + 'bookmaker_markets.sql'
	if os.path.exists(sql_path):
		sql = filterEvents(sql_path)



def filterEvents(sql_path):
	global events
	final = ''

	try:
		events_lines_to_double_check = []
		query = 'SELECT e.id, e.date, e.time, e.title, e.fk_tournament_id, et.fk_team_id FROM event_teams et LEFT JOIN events e ON e.id = et.fk_event_id WHERE e.teams_count = 2 AND e.related_to_market IS NULL'
		
		#cursor.execute("")

		with open(sql_path) as file:
			i = 0
			for line in file:
				found = False
				m = re.search('{teams=(.*)}', line)

				if m.group(0):
					teams_ids = m.group(1).strip().split(',')

					if teams_ids and len(teams_ids) == 2:
						_line = line.lstrip(',')
						event_data = _line.split(',')
						tournament_id = event_data[1]
						title = event_data[3]
						date = event_data[4]
						date = datetime.datetime.strptime(date, MYSQL_DATE_FORMAT)

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
										found = True
										break
				
				if not found:
					query += " OR WHERE (e.fk_tournament_id =" + tournament_id + " AND et.fk_team_id IN (" + ", ".join(teams_ids) + "))"
					events_lines_to_double_check.append(i)
				else:
					line = re.sub('{teams=(.*)}', '', line)

				final += line
				i += 1

		# Double check
		with open(sql_path) as file:
			i = 0
			for line in file:

	return final