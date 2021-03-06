import bookmaker_consumer
import sys
import re
import psycopg2
import os
from os import walk
import shutil
import csv
import datetime
import json
from datetime import date, timedelta
sys.path.append("../")
sys.path.append("../../")
from scripts import MarketParser
from models import MarketRule, OutcomeRule, BookmakerEventTeam

# Constants
NOT_PROCESSED_STRUCTURE = {'mapped': {}, 'not_mapped': {}, 'error': {}}
MYSQL_DATE_FORMAT = '%Y-%m-%d'
MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
INACTIVE = 0

entities = ['sports', 'tournaments', 'teams', 'markets']

bookmaker_sports = {}
bookmaker_tournaments = {}
bookmaker_teams = {}
bookmaker_markets = {}

sports_maps = {}
tournaments_maps = {}
teams_maps = {}
markets_maps = {}

saved_outcomes = []
queue_path = None
queue_csv_path = None

market_parser = None

bookmaker_id = None
bookmaker_title = None

connection = None

events = {}
processed_events = []
not_processed_outcomes = NOT_PROCESSED_STRUCTURE
bookmaker_update_queues_sql = ''

def run(id, title, timestamp, live, started_at):
	global bookmaker_id
	global bookmaker_title
	global queue_path
	global connection
	global bookmaker_update_queues_sql
	global saved_outcomes

	if connection == None:
		connection = psycopg2.connect(user = "postgres",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet_prod")

	live = live == 'True'
	bookmaker_id = id
	bookmaker_title = title
	queue_csv_path = '../../queues/Readers/' + bookmaker_title + '/' + timestamp + '/'
	not_processed_outcomes = NOT_PROCESSED_STRUCTURE
	bookmaker_update_queues_sql = 'INSERT INTO bookmaker_update_queues VALUES \n'
	updated_at = datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)

	# Init bookmaker entities
	initBookmakerEntities(title)
	initMappings(title)
	initMarketParser(title)
	initEvents()

	if os.path.exists(queue_csv_path):
		pages = []

		for (dirpath, dirnames, files_names) in walk(queue_csv_path):
			pages.extend(dirnames)
			break

		row_index = 0
		for page in pages:
			queue_path = '../../queues/Readers/' + bookmaker_title + '/' + timestamp + '/' + page + '/'

			if os.path.exists(queue_path):
				print('Processing page ' + page)
				try:
					saved_outcomes = []
					picket_at = datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
					seedDB()
					now = datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
					if row_index > 0:
						bookmaker_update_queues_sql += '\n,'

					bookmaker_update_queues_sql += '(DEFAULT, ' + str(bookmaker_id) + ', NULL, \'' + queue_path + '\', ' + page  + ', 0, 1, 0, 0, NULL, \'' + json.dumps(not_processed_outcomes) + '\', \'' + now + '\', \'' + now + '\', \'' + picket_at + '\', \'' + started_at + '\', \'' + json.dumps(saved_outcomes) + '\')\n' 
					row_index += 1
				except (Exception) as ex:
					pass

		# Delete download folder
		shutil.rmtree(queue_csv_path)

	if len(bookmaker_update_queues_sql) > 0:
		try:
			cursor = connection.cursor()
			cursor.execute(bookmaker_update_queues_sql)
			connection.commit()
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			connection.rollback()

	# Update status
	try:
		cursor = connection.cursor()
		cursor.execute('UPDATE bookmaker_statuses SET started_at = \'' + started_at.replace('@', ' ') + '\', updated_at = \'' + updated_at + '\' WHERE fk_bookmaker_id = ' + str(bookmaker_id) + ' AND live = ' + ('1' if live else '0'))
		connection.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		connection.rollback()

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
		csv_path = '../cache/entities/' + title + '/' + entity + '.csv'
		if os.path.exists(csv_path):
			with open(csv_path, 'r', encoding="utf-8") as file:
				for line in file:
					row = line.strip().split('@s.s@')
					try:
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
								'outcomes': [] if row[5] == '[]' or len(row[5]) == 0 else row[5]
							}
					except:
						pass


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
		csv_path = '../cache/mappings/' + title + '/' + entity + '.csv'
		if os.path.exists(csv_path):
			with open(csv_path, 'r', encoding="utf-8") as file:
				for line in file:
					row = line.strip().split('@s.s@')
					try:
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
							# entity_id@s.s@entity_title@s.s@entity_display_title@s.s@entity_parent_id@s.s@entity_parent_title@s.s@bookmaker_entity_id@s.s@bookmaker_entity_title
							markets_maps[row[5]] = {
								'sport_id': row[3],
								'sport_title': row[4],
								'market_id': row[0],
								'market_title': row[1],
								'market_display_title': row[2],
								'bookmaker_market_id': row[5],
								'bookmaker_market_title': row[6],
							}
					except:
						pass

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
	csv_path = '../cache/market_parser/markets/' + title + '/rules.csv'
	rules = []
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				try:
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

				except:
					pass

	market_parser.setMarketsRules(rules)

	# Get outcome rules belonging to the current bookmaker
	csv_path = '../cache/market_parser/outcomes/' + title + '/rules.csv'
	rules = []
	if os.path.exists(csv_path):
		with open(csv_path, 'r', encoding="utf-8") as file:
			for line in file:
				try:
					row = line.strip().split('@s.s@')
					# market_id@s.s@market_title@s.s@sport_id@s.s@sport_title@s.s@id@s.s@input@s.s@input_replace@s.s@outcome_name@s.s@outcome_save_patter@s.s@outcome_output
					rule = OutcomeRule.OutcomeRule()
					input_replace = []
					_input_replace = json.loads(row[6]) if row[6] != 'None' else []

					rule.id = row[4]
					rule.sport_title = row[3]
					rule.market_title = row[1]
					rule.input = row[5]
					rule.input_replace = _input_replace
					rule.output = row[8]

					rules.append(rule)
				except:
					pass

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
			#print('Seeding bookmaker sports')
			file = open(sql_path, 'r', encoding="utf-8")
			sql = file.read()
			cursor = connection.cursor()
			cursor.execute(sql)
			connection.commit()
			sql = ''
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
			#print('Seeding bookmaker tournaments')
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
			sql = ''
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
			#print('Seeding bookmaker teams')
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
			sql = ''
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
			#print('Seeding bookmaker markets')
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
			sql = ''
		except (Exception, psycopg2.DatabaseError) as error:
			print('Could not insert bookmaker markets: ' + str(error))
			connection.rollback()

def seedEvents():
	global queue_path
	global connection
	global bookmaker_sports
	global processed_events

	sql_path = queue_path + 'events.sql'
	if os.path.exists(sql_path):
		sql = filterEvents(sql_path)

		if len(sql) > 0:
			try:
				#print('Seeding events')
				cursor = connection.cursor()
				cursor.execute(sql)
				connection.commit()
			except (Exception, psycopg2.DatabaseError) as error:
				print('Could not insert events: ' + str(error))
				connection.rollback()

		sql = ''

		# Get inserted events
		if len(processed_events) > 0:
			query = 'SELECT s.title as sport_title, e.id as event_id, t.title as tournament_title, e.title as event_title, e.date as event_date, e.time as event_time FROM events e LEFT JOIN tournaments t ON t.id = e.fk_tournament_id LEFT JOIN sports s ON s.id = t.fk_sport_id WHERE '
			i = 0

			for event in processed_events:
				if i > 0:
					query += ' OR '

				if event['outrights']:
					query += '(e.fk_tournament_id = ' + event['tournament_id'] + ' and e.title = \'' + event['title'] + '\')'
				else:
					query += '(e.fk_tournament_id = ' + event['tournament_id'] + ' and e.title = \'' + event['title'] + '\' and e.date = \'' + event['date'] + '\')'
				
				i += 1
			
			records = []
			try:
				cursor = connection.cursor()
				cursor.execute(query)
				records = cursor.fetchall()
			except:
				pass

			if len(records) > 0:
				# Process event teams + bookmaker events
				event_teams_sql = ''
				bookmaker_events_sql = ''
				ids = []

				if os.path.exists(queue_path + 'event_teams.sql'):
					file = open(queue_path + 'event_teams.sql', 'r', encoding="utf-8")
					event_teams_sql = file.read()

				if os.path.exists(queue_path + 'bookmaker_events.sql'):
					file = open(queue_path + 'bookmaker_events.sql', 'r', encoding="utf-8")
					bookmaker_events_sql = file.read()

				processed_events = {}
				for row in records:
					event_id = str(row[1])
					date = str(row[4])
					time = str(row[5])
					datetime = date + ' ' + time

					ids.append(event_id)
					processed_events[row[1]] = {
						'sport_title': row[0],
						'tournament_title': row[2],
						'title': row[3],
						'date': date,
						'time': time
					}

					event_teams_sql = event_teams_sql.replace('{sport=' + row[0] + '&tournament=' + row[2] + '&event=' + row[3] + '&date=' + datetime + '}', event_id)
					bookmaker_events_sql = bookmaker_events_sql.replace('{sport=' + row[0] + '&tournament=' + row[2] + '&event=' + row[3] + '&date=' + datetime + '}', event_id)

				# Remove all lines that haven't been replaced
				event_teams_sql = re.sub('\n?,?\(DEFAULT, {sport=.*&tournament=.*&event=.*&date=.*},.*\)', '', event_teams_sql)
				bookmaker_events_sql = re.sub('\n?,?\(DEFAULT, \d+, {sport=.*&tournament=.*&event=.*&date=.*},.*\)', '', bookmaker_events_sql)

				lines = event_teams_sql.split('\n')
				i = 0
				final = ''

				for line in lines:
					if i == 1:
						line = line.lstrip(',')

					final += line
					i += 1

				event_teams_sql = final

				lines = bookmaker_events_sql.split('\n')
				i = 0
				final = ''

				for line in lines:
					if i == 1:
						line = line.lstrip(',')

					final += line
					i += 1

				bookmaker_events_sql = final
				final = ''

				# Seed event teams
				try:
					#print('Seeding event teams')
					cursor.execute(event_teams_sql)
					connection.commit()
				except (Exception, psycopg2.DatabaseError) as error:
					print('Could not insert event teams: ' + str(error))
					connection.rollback()

				# Seed bookmaker events
				insert_bookmaker_event_markets = False
				try:
					#print('Seeding bookmaker events')
					cursor.execute(bookmaker_events_sql)
					connection.commit()
					insert_bookmaker_event_markets = True
				except (Exception, psycopg2.DatabaseError) as error:
					print('Could not insert bookmaker events: ' + str(error))
					connection.rollback()

				event_teams_sql = ''
				bookmaker_events_sql = ''

				if insert_bookmaker_event_markets:
					seedBookmakerEventMarkets(ids)

def filterEvents(sql_path):
	global events
	global processed_events

	output = ""
	last_line = ""
	processed_events = []

	try:
		events_lines_to_double_check = []
		query = 'SELECT e.id, e.date, e.time, e.title, e.fk_tournament_id, et.fk_team_id FROM event_teams et LEFT JOIN events e ON e.id = et.fk_event_id WHERE e.teams_count = 2 AND e.related_to_market IS NULL'

		with open(sql_path, encoding="utf-8") as file:
			i = 0
			for line in file:
				found = False

				if i == 0:
					output += line
					i += 1
					continue
				elif line.startswith('ON CONFLICT'):
					last_line = line
					continue

				m = re.search('{teams=(.*)}', line)
				teams_ids = []

				if m.group(0):
					teams_ids = m.group(1).strip().split(',')
					_line = line.lstrip(',')
					event_data = _line.split(',')
					tournament_id = event_data[1].strip()
					title = event_data[3].replace("'", "")
					_date = event_data[4].replace("'", "")
					time = event_data[12].replace("'", "")
					_date = _date.strip()
					time = time.strip()
					_date = datetime.datetime.strptime(_date, MYSQL_DATE_FORMAT)

					if len(teams_ids) == 2:
						# Check current, previous and next dates
						today = datetime.datetime.now().strftime(MYSQL_DATE_FORMAT)
						two_days_ago_date = (_date - timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)
						previous_date = (_date - timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
						next_date = (_date + timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
						two_days_after_date = (_date + timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)

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

				line = re.sub('{teams=(.*)}', '', line)

				if not found and len(teams_ids) == 2:
					query += " OR (e.fk_tournament_id = " + tournament_id + " AND et.fk_team_id IN (" + ", ".join(teams_ids) + "))"
					events_lines_to_double_check.append({
						'tournament_id': tournament_id,
						'title': title,
						'date': _date.strftime(MYSQL_DATE_FORMAT),
						'time': time,
						'teams_ids': teams_ids,
						'line': line
					})
				elif len(teams_ids) > 2: # Outrights
					if not line.startswith(',') and len(processed_events) > 0:
						line = ',' + line
					elif line.startswith(',') and len(processed_events) == 0:
						line = line.lstrip(',')

					output += line
					processed_events.append({
						'tournament_id': tournament_id,
						'title': title.lstrip(),
						'date': _date.strftime(MYSQL_DATE_FORMAT),
						'time': time,
						'outrights': True
					})

				i += 1

		# Double check
		if len(events_lines_to_double_check) > 0:
			cursor = connection.cursor()
			cursor.execute(query)
			records = cursor.fetchall()

			if len(records) > 0:
				for row in records:
					event_id = str(row[0])
					event_date = str(row[1])
					event_time = str(row[2])
					event_title = row[3]
					tournament_id = str(row[4])
					event_team_id = str(row[5])

					if not tournament_id in events:
						events[tournament_id] = {}

					if not event_date in events[tournament_id]:
						events[tournament_id][event_date] = {}

					if not event_id in events[tournament_id][event_date]:
						events[tournament_id][event_date][event_id] = {
							'teams': [],
							'date': event_date,
							'datetime': event_date + ' ' + event_time,
							'title': event_title
						}

					if event_team_id not in events[tournament_id][event_date][event_id]['teams']:
						events[tournament_id][event_date][event_id]['teams'].append(event_team_id)

			for event_to_double_check in events_lines_to_double_check:
				tournament_id = event_to_double_check['tournament_id']
				teams_ids = event_to_double_check['teams_ids']
				_date = event_to_double_check['date']
				_date = datetime.datetime.strptime(_date, MYSQL_DATE_FORMAT)
				# Check current, previous and next dates
				today = datetime.datetime.now().strftime(MYSQL_DATE_FORMAT)
				two_days_ago_date = (_date - timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)
				previous_date = (_date - timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
				next_date = (_date + timedelta(days=1)).strftime(MYSQL_DATE_FORMAT)
				two_days_after_date = (_date + timedelta(days=2)).strftime(MYSQL_DATE_FORMAT)

				dates_to_check = [today, previous_date, next_date, two_days_after_date, two_days_ago_date]
				found = False

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
					line = event_to_double_check['line']

					if not line.startswith(',') and len(processed_events) > 0:
						line = ',' + line
					elif line.startswith(',') and len(processed_events) == 0:
						line = line.lstrip(',')

					output += line
					processed_events.append({
						'tournament_id': tournament_id,
						'title': event_to_double_check['title'].strip(),
						'date': event_to_double_check['date'],
						'time': event_to_double_check['time'],
						'outrights': False
					})

		output += last_line
	except (Exception) as ex:
		pass

	return output

def seedBookmakerEventMarkets(ids):
	if os.path.exists(queue_path + 'bookmaker_event_markets.sql'):
		file = open(queue_path + 'bookmaker_event_markets.sql', 'r', encoding="utf-8")
		sql = file.read()

		# Get inserted bookmaker events
		query = "SELECT be.id as bookmaker_event_id, e.title as event_title, e.date as event_date, e.time as event_time, t.title as tournament_title, s.title as sport_title FROM bookmaker_events be LEFT JOIN events e ON e.id = be.fk_event_id LEFT JOIN tournaments t ON t.id = e.fk_tournament_id LEFT JOIN sports s ON s.id = t.fk_sport_id WHERE fk_bookmaker_id = " + str(bookmaker_id) + " AND fk_event_id IN (" + ", ".join(ids) + ")"
		records = []
		try:
			cursor = connection.cursor()
			cursor.execute(query)
			records = cursor.fetchall()
		except:
			pass

		if len(records) > 0:
			for row in records:
				datetime = str(row[2]) + ' ' + str(row[3])
				sql = sql.replace('{sport=' + row[5] + '&tournament=' + row[4] + '&event=' + row[1] + '&date=' + datetime + '}', str(row[0]))

			# Remove all lines that haven't been replaced
			sql = re.sub('\n?\,?\(DEFAULT\, \{sport=(.*)&tournament=(.*)&event=(.*)&date=(.*)\}\,.*\)', '', sql)

			lines = sql.split('\n')
			i = 0
			final = ''

			for line in lines:
				if i == 1:
					line = line.lstrip(',')

				final += line
				i += 1

			sql = final
			final = ''

			# Seed bookmaker event markets
			insert_bookmaker_event_market_outcomes = False
			try:
				#print('Seeding bookmaker event markets')
				cursor.execute(sql)
				connection.commit()
				insert_bookmaker_event_market_outcomes = True
			except (Exception, psycopg2.DatabaseError) as error:
				print('Could not insert bookmaker event markets: ' + str(error))
				connection.rollback()

			sql = ''

			if insert_bookmaker_event_market_outcomes:
				seedBookmakerEventMarketOutcomes(ids)

def seedBookmakerEventMarketOutcomes(ids):
	global bookmaker_sports
	global bookmaker_tournaments
	global bookmaker_markets
	global sports_maps
	global tournaments_maps
	global teams_maps
	global markets_maps
	global market_parser
	global saved_outcomes

	not_processed_outcomes = NOT_PROCESSED_STRUCTURE

	if os.path.exists(queue_path + 'bookmaker_event_market_outcomes.sql'):
		sql = ''
		last_line = ''
		i = 0

		# Get inserted bookmaker event markets
		query = "SELECT bem.id as bookmaker_event_market_id, bem.title as bookmaker_event_market_title, be.title as bookmaker_event_title, e.date as event_date, e.time as event_time, e.fk_tournament_id as tournament_id, e.id as event_id FROM bookmaker_event_markets bem LEFT JOIN bookmaker_events be ON be.id = bem.fk_bookmaker_event LEFT JOIN events e ON e.id = be.fk_event_id WHERE be.fk_bookmaker_id = " + str(bookmaker_id) + " AND e.id IN (" + ", ".join(ids) + ")"
		records = []
		new_bookmaker_teams = []
		outcomes = {}
		not_processed = NOT_PROCESSED_STRUCTURE

		try:
			cursor = connection.cursor()
			cursor.execute(query)
			records = cursor.fetchall()
		except:
			pass

		if len(records) > 0:
			with open(queue_path + 'bookmaker_event_market_outcomes.sql', encoding="utf-8") as file:
				i = 0
				regex = '\n?\,?\(DEFAULT\, \{sport=(.*)&tournament=(.*)&event=(.*)&date=(.*)&market_title=(.*)&teams=(.*)\}\, \{team_id\}\,.*\{outcome_title=(.*)\}\,.*\)'
				for line in file:
					found = False

					if i == 0:
						sql += line.rstrip('\n')
						i += 1
						continue
					elif line.startswith('ON CONFLICT'):
						last_line = line
						continue

					match = re.search(regex, line)
					output = match.group(0)
					bookmaker_sport_title = match.group(1)
					bookmaker_tournament_title = match.group(2)
					event_title = match.group(3)
					event_date = match.group(4)
					bookmaker_market_title = match.group(5)
					teams_titles = match.group(6)
					outcome_title = match.group(7)
					not_mapped = True
					replaced = False
					duplicated = False

					#print('Preg replace ' + event_title + ' => ' + bookmaker_market_title + ' // ' + outcome_title)

					teams = json.loads(teams_titles)
					event_teams = []

					if len(teams) > 0:
						i = 0
						for _team_title in teams:
							team = BookmakerEventTeam.BookmakerEventTeam()

							team.title = _team_title
							team.local = i == 0

							event_teams.append(team)
							i += 1

					if (
						bookmaker_sport_title in bookmaker_sports
						and bookmaker_sport_title in bookmaker_tournaments
						and bookmaker_tournament_title in bookmaker_tournaments[bookmaker_sport_title]
						and bookmaker_sport_title in bookmaker_markets
						and bookmaker_market_title in bookmaker_markets[bookmaker_sport_title]
					):
						bookmaker_sport_id = bookmaker_sports[bookmaker_sport_title]['id']
						sport_title = sports_maps[bookmaker_sport_id]['sport_title']
						tournament_id = tournaments_maps[bookmaker_tournaments[bookmaker_sport_title][bookmaker_tournament_title]['id']]['tournament_id']
						tournament_title = tournaments_maps[bookmaker_tournaments[bookmaker_sport_title][bookmaker_tournament_title]['id']]['tournament_title']
						bookmaker_market = bookmaker_markets[bookmaker_sport_title][bookmaker_market_title]

						market_rule = market_parser.getMarketRule(bookmaker_market_title, sport_title)
						if market_rule:
							market_map = {
								'sport_id': market_rule.sport_id,
								'sport_title': market_rule.sport_title,
								'market_id': market_rule.market_id,
								'market_title': market_rule.market_title,
								'bookmaker_market_id': bookmaker_market['id'],
								'bookmaker_market_title': bookmaker_market['title'],
							}

							markets_maps[bookmaker_market['id']] = market_map

						market = markets_maps[bookmaker_market['id']]
						market_title = market['market_title']
						market_display_title = market['market_display_title'] if 'market_display_title' in market else market_title
						for event_id in processed_events:
							processed_event = processed_events[event_id]
							_datetime = processed_event['date'] + ' ' + processed_event['time']

							if (
								processed_event['sport_title'] == sport_title
								and processed_event['tournament_title'] == tournament_title
								and processed_event['title'] == event_title
								and _datetime == event_date
							):
								# Find corresponding bookmaker event market
								for row in records:
									bookmaker_event_market_id = row[0]
									if (
										row[1] == market_title
										and row[6] == event_id
									):
										# Parse outcome
										try:
											team_id = 'NULL'
											market_parser.setBookmakerTeams(bookmaker_teams[bookmaker_sport_title])
											outcome = market_parser.getMarketOutcome(outcome_title, sport_title, market_title, event_teams)
											missing_bookmaker_teams = market_parser.getMissingBookmakerTeams()

											if len(outcome) == 0 and len(missing_bookmaker_teams) > 0:
												for missing_bookmaker_team in missing_bookmaker_teams:
													new_bookmaker_teams.append({
														'fk_bookmaker_id': bookmaker_id,
														'fk_bookmaker_sport_id': bookmaker_sport_id,
														'title': missing_bookmaker_team,
														'found_in': event_title 
													})
											elif (
												outcome_title in bookmaker_teams[bookmaker_sport_title]
												and bookmaker_teams[bookmaker_sport_title][outcome_title]['id'] in teams_maps
											):
												team_id = teams_maps[bookmaker_teams[bookmaker_sport_title][outcome_title]['id']]['team_id']

											if bookmaker_event_market_id not in outcomes:
												outcomes[bookmaker_event_market_id] = []

											if outcome in outcomes[bookmaker_event_market_id]:
												duplicated = True

											if (
												(len(outcome) > 0 or outcome.isnumeric())
												and outcome not in outcomes[bookmaker_event_market_id]
											):
												if output.find('{sport=' + bookmaker_sport_title + '&tournament=' + bookmaker_tournament_title + '&event=' + event_title + '&date=' + event_date + '&market_title=' + bookmaker_market_title + '&teams=' + teams_titles + '}') > -1:
													matching_outcome_rule = market_parser.getMatchingOutcomeRule()
													now = datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
													output = output.replace('{sport=' + bookmaker_sport_title + '&tournament=' + bookmaker_tournament_title + '&event=' + event_title + '&date=' + event_date + '&market_title=' + bookmaker_market_title + '&teams=' + teams_titles + '}', str(bookmaker_event_market_id))
													output = output.replace('{team_id}', team_id)
													output = output.replace('{outcome_title=' + outcome_title + '}', "'" + outcome + "'")
													output = output.replace('{outcome_rule_id}', matching_outcome_rule.id)
													output = output.replace('{created_at}', now)
													output = output.replace('{updated_at}', now)
													sql += '\n' + output.lstrip(',') + ','
													outcomes[bookmaker_event_market_id].append(outcome)
													not_mapped = False
													replaced = True

													saved_outcomes.append({
														'market': market_display_title,
														'bookmaker_market': bookmaker_market_title,
														'outcome': outcome_title.replace("'", "´"),
														'sport': sport_title,
														'tournament': bookmaker_tournament_title.replace("'", "´"),
														'event': event_title,
														'output': outcome,
														'rule': matching_outcome_rule.id,
														'datetime': now
													})
											if not replaced and len(missing_bookmaker_teams) == 0 and outcome not in outcomes[bookmaker_event_market_id]:
												not_mapped = False

												if bookmaker_market_title not in not_processed['mapped']:
													not_processed['mapped'][bookmaker_market_title] = {}

												if bookmaker_event_market_id not in not_processed['mapped'][bookmaker_market_title]:
													not_processed['mapped'][bookmaker_market_title][bookmaker_event_market_id] = []

												not_processed['mapped'][bookmaker_market_title][bookmaker_event_market_id].append({
													'market': market_display_title,
													'bookmaker_market': bookmaker_market_title,
													'outcome': outcome_title.replace("'", "´"),
													'sport': sport_title,
													'tournament': bookmaker_tournament_title.replace("'", "´"),
													'event': event_title,
													'output': outcome,
													'final_outcomes': outcomes[bookmaker_event_market_id],
													'datetime': datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
												})

										except (Exception) as error:
											if bookmaker_event_market_id not in not_processed['error']:
												not_processed['error'][bookmaker_event_market_id] = []
										
											not_mapped = False
											now = datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
											not_processed['error'][bookmaker_event_market_id].append({
												'market': market_display_title,
												'bookmaker_market': bookmaker_market_title,
												'outcome': outcome_title.replace("'", "´"),
												'sport': sport_title,
												'tournament': bookmaker_tournament_title.replace("'", "´"),
												'event': event_title,
												'error': str(error),
												'datetime': datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT)
											})
											pass
										break
								break

					if not_mapped and not duplicated:
						if bookmaker_market_title not in not_processed['not_mapped']:
							not_processed['not_mapped'][bookmaker_market_title] = {}

						if event_title not in not_processed['not_mapped'][bookmaker_market_title]:
							not_processed['not_mapped'][bookmaker_market_title][event_title] = []

						not_processed['not_mapped'][bookmaker_market_title][event_title].append({
							'outcome': outcome_title.replace("'", "´"),
							'sport': bookmaker_sport_title,
							'tournament': bookmaker_tournament_title,
							'datetime': datetime.datetime.now().strftime(MYSQL_DATETIME_FORMAT) 
						})

				not_processed_outcomes = not_processed
				sql = sql.rstrip(',')
				sql += '\n' + last_line

				try:
					#print('Seeding bookmaker event market outcomes')
					cursor.execute(sql)
					connection.commit()
				except (Exception, psycopg2.DatabaseError) as error:
					print('Could not insert bookmaker event market outcomes: ' + str(error))
					connection.rollback()

				sql = ''

				# Insert new bookmaker teams
				if len(new_bookmaker_teams) > 0:
					sql = 'INSERT INTO bookmaker_teams VALUES \n'
					i = 0

					for team in new_bookmaker_teams:
						if i > 0:
							sql += '\n,'

						sql += "(DEFAULT, " + str(team['fk_bookmaker_id']) + ", " + team['fk_bookmaker_sport_id'] + ", '" + team['title'].replace("'", "´") + "', '" + team['found_in'].replace("'", "´") + "', " + str(INACTIVE) + ", " + str(INACTIVE) + ", '" + date.today().strftime(MYSQL_DATETIME_FORMAT) + "')"
						i += 1

					sql += '\nON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;'

					try:
						cursor.execute(sql)
						connection.commit()
					except (Exception, psycopg2.DatabaseError) as error:
						print('Could not insert new bookmaker teams: ' + str(error))
						connection.rollback()
