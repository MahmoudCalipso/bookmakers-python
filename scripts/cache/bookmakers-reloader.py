# Reload data used by bookmaker updater and consumer

import psycopg2
import os
import time

start_time = time.time()

try:
	connection = psycopg2.connect(user = "asanchez",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet")

	cursor = connection.cursor()

	# Events
	cursor.execute("SELECT e.id, e.date, e.time, e.title, e.fk_tournament_id, et.fk_team_id FROM event_teams et LEFT JOIN events e ON e.id = et.fk_event_id WHERE e.teams_count = 2 AND e.related_to_market IS NULL")
	records = cursor.fetchall()
	csv_rows = ''

	for row in records:			
		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '\n'
	
	with open('events.csv', 'w', encoding="utf-8") as fd:
		fd.write(csv_rows)
	
	# Bookmaker sports
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_id, bs.title as entity_title, ignore as skip_entity FROM bookmaker_sports bs LEFT JOIN bookmakers b ON b.id = bs.fk_bookmaker_id order by b.title")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[0]
		if not os.path.exists('entities/' + bookmaker_title):
			os.makedirs('entities/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('entities/' + current_bookmaker + '/sports.csv'):
				os.remove('entities/' + current_bookmaker + '/sports.csv')

			csv_path = 'entities/' + current_bookmaker + '/sports.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '\n'
		i += 1

	# Sports mapping
	cursor.execute("SELECT s.id as entity_id, s.title as entity_title, s.live_date_interval as entity_live_date_interval, bs.id as bookmaker_entity_id, bs.title as bookmaker_entity_title, b.id as bookmaker_id, b.title as bookmaker_title FROM sports_map sm LEFT JOIN sports s ON s.id = sm.fk_sport_id LEFT JOIN bookmaker_sports bs ON bs.id = sm.fk_bookmaker_sport LEFT JOIN bookmakers b ON b.id = bs.fk_bookmaker_id order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[6]
		if not os.path.exists('mappings/' + bookmaker_title):
			os.makedirs('mappings/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('mappings/' + current_bookmaker + '/sports.csv'):
				os.remove('mappings/' + current_bookmaker + '/sports.csv')

			csv_path = 'mappings/' + current_bookmaker + '/sports.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '\n'
		i += 1

	# Bookmaker tournaments
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, skip as skip_entity FROM bookmaker_tournaments bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()
	csv_rows = '';
	current_bookmaker = None
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:
			if os.path.exists('entities/' + current_bookmaker + '/tournaments.csv'):
				os.remove('entities/' + current_bookmaker + '/tournaments.csv')

			csv_path = 'entities/' + current_bookmaker + '/tournaments.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '\n'
		i += 1

	# Tournaments mapping
	cursor.execute("SELECT t.id as entity_id, t.title as entity_title, s.id as entity_parent_id, s.title as entity_parent_title, bt.id as bookmaker_entity_id, bt.title as bookmaker_entity_title, b.id as bookmaker_id, b.title as bookmaker_title FROM tournaments_map tm LEFT JOIN tournaments t ON t.id = tm.fk_tournament_id LEFT JOIN bookmaker_tournaments bt ON bt.id = tm.fk_bookmaker_tournament_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id LEFT JOIN sports s ON s.id = t.fk_sport_id order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[7]
		if not os.path.exists('mappings/' + bookmaker_title):
			os.makedirs('mappings/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('mappings/' + current_bookmaker + '/tournaments.csv'):
				os.remove('mappings/' + current_bookmaker + '/tournaments.csv')

			csv_path = 'mappings/' + current_bookmaker + '/tournaments.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '\n'
		i += 1

	# Bookmaker teams
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, bt.ignore as skip_entity FROM bookmaker_teams bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()
	csv_rows = '';
	current_bookmaker = None
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:
			if os.path.exists('entities/' + current_bookmaker + '/teams.csv'):
				os.remove('entities/' + current_bookmaker + '/teams.csv')

			csv_path = 'entities/' + current_bookmaker + '/teams.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '\n'
		i += 1

	# Teams mapping
	cursor.execute("SELECT t.id as entity_id, t.title as entity_title, s.id as entity_parent_id, s.title as entity_parent_title, bt.id as bookmaker_entity_id, bt.title as bookmaker_entity_title, b.id as bookmaker_id, b.title as bookmaker_title FROM teams_map tm LEFT JOIN teams t ON t.id = tm.fk_team_id LEFT JOIN bookmaker_teams bt ON bt.id = tm.fk_bookmaker_team LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id LEFT JOIN sports s ON s.id = t.fk_sport_id order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[7]
		if not os.path.exists('mappings/' + bookmaker_title):
			os.makedirs('mappings/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('mappings/' + current_bookmaker + '/teams.csv'):
				os.remove('mappings/' + current_bookmaker + '/teams.csv')

			csv_path = 'mappings/' + current_bookmaker + '/teams.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '\n'
		i += 1

	# Bookmaker markets
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, bt.skip as skip_entity, bt.outcomes as entity_outcomes FROM bookmaker_markets bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()
	csv_rows = '';
	current_bookmaker = None
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:
			if os.path.exists('entities/' + current_bookmaker + '/markets.csv'):
				os.remove('entities/' + current_bookmaker + '/markets.csv')

			csv_path = 'entities/' + current_bookmaker + '/markets.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '@s.s@' + str(row[6]) + '\n'
		i += 1

	# Markets mapping
	cursor.execute("SELECT m.id as entity_id, m.title as entity_title, m.display_title as entity_display_title, s.id as entity_parent_id, s.title as entity_parent_title, bm.id as bookmaker_entity_id, bm.title as bookmaker_entity_title, b.id as bookmaker_id, b.title as bookmaker_title FROM markets_map mm LEFT JOIN markets m ON m.id = mm.fk_market_id LEFT JOIN bookmaker_markets bm ON bm.id = mm.fk_bookmaker_market LEFT JOIN bookmakers b ON b.id = bm.fk_bookmaker_id LEFT JOIN sports s ON s.id = m.fk_sport_id order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	i = 0
	total = cursor.rowcount
	
	for row in records:
		bookmaker_title = row[8]
		if not os.path.exists('mappings/' + bookmaker_title):
			os.makedirs('mappings/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('mappings/' + current_bookmaker + '/markets.csv'):
				os.remove('mappings/' + current_bookmaker + '/markets.csv')

			csv_path = 'mappings/' + current_bookmaker + '/markets.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '@s.s@' + str(row[3]) + '@s.s@' + str(row[4]) + '@s.s@' + str(row[5]) + '@s.s@' + str(row[6]) + '\n'
		i += 1

except (Exception, psycopg2.Error) as error :
	print ("Error while connecting to PostgreSQL", error)
finally:
	#closing database connection.
		if(connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))