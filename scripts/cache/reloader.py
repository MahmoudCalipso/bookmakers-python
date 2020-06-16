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
	
	# Bookmaker sports
	cursor.execute("SELECT b.title as bookmaker_title, bs.title as entity_title, ignore as skip_entity FROM bookmaker_sports bs LEFT JOIN bookmakers b ON b.id = bs.fk_bookmaker_id order by b.title")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = '';
	
	for row in records:
		bookmaker_title = row[0]
		if not os.path.exists('entities/' + bookmaker_title):
			os.makedirs('entities/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title:

			if os.path.exists('entities/' + current_bookmaker + '/sports.csv'):
				os.remove('entities/' + current_bookmaker + '/sports.csv')

			csv_path = 'entities/' + current_bookmaker + '/sports.csv'
			with open(csv_path, 'a') as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + ';' + str(row[2]) + '\n'

	# Bookmaker tournaments
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, skip as skip_entity FROM bookmaker_tournaments bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()

	csv_rows = '';
	current_bookmaker = None
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title:
			if os.path.exists('entities/' + current_bookmaker + '/tournaments.csv'):
				os.remove('entities/' + current_bookmaker + '/tournaments.csv')

			csv_path = 'entities/' + current_bookmaker + '/tournaments.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + ';' + str(row[2]) + ';' + str(row[3]) + ';' + str(row[4]) + ';' + str(row[5]) + '\n'

	# Bookmaker teams
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, bt.ignore as skip_entity FROM bookmaker_teams bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()

	csv_rows = '';
	current_bookmaker = None
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title:
			if os.path.exists('entities/' + current_bookmaker + '/teams.csv'):
				os.remove('entities/' + current_bookmaker + '/teams.csv')

			csv_path = 'entities/' + current_bookmaker + '/teams.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + ';' + str(row[2]) + ';' + str(row[3]) + ';' + str(row[4]) + ';' + str(row[5]) + '\n'

	# Bookmaker markets
	cursor.execute("SELECT b.title as bookmaker_title, bs.id as entity_parent_id, bs.title as entity_parent_title, bt.id as entity_id, bt.title as entity_title, bt.skip as skip_entity FROM bookmaker_markets bt LEFT JOIN bookmaker_sports bs ON bs.id = bt.fk_bookmaker_sport_id LEFT JOIN bookmakers b ON b.id = bt.fk_bookmaker_id ORDER BY b.title, bt.fk_bookmaker_sport_id")
	records = cursor.fetchall()

	csv_rows = '';
	current_bookmaker = None
	
	for row in records:
		bookmaker_title = row[0]
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title:
			if os.path.exists('entities/' + current_bookmaker + '/markets.csv'):
				os.remove('entities/' + current_bookmaker + '/markets.csv')

			csv_path = 'entities/' + current_bookmaker + '/markets.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[1]) + ';' + str(row[2]) + ';' + str(row[3]) + ';' + str(row[4]) + ';' + str(row[5]) + '\n'

except (Exception, psycopg2.Error) as error :
	print ("Error while connecting to PostgreSQL", error)
finally:
	#closing database connection.
		if(connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))