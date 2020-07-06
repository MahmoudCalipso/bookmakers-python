# Reload data used by bookmaker updater and consumer

import psycopg2
import os
import time
import csv

start_time = time.time()

try:
	connection = psycopg2.connect(user = "postgres",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet_prod")

	cursor = connection.cursor()

	# Constants
	cursor.execute("SELECT id, variable, value from constants")
	records = cursor.fetchall()
	csv_rows = ''

	for row in records:			
		csv_rows += str(row[0]) + '@s.s@' + str(row[1]) + '@s.s@' + str(row[2]) + '\n'
	
	with open('constants.csv', 'w', encoding="utf-8") as fd:
		fd.write(csv_rows)

	# Market rules
	cursor.execute("select b.id as bookmaker_id, b.title as bookmaker_title, s.id as sport_id, s.title as sport_title, m.id as market_id, m.title as market_title, input, input_replace, outcome_output from bookmaker_market_rules bmr LEFT JOIN bookmakers b ON b.id = bmr.fk_bookmaker_id LEFT JOIN sports s ON s.id = bmr.fk_sport_id LEFT JOIN markets m ON m.id = bmr.fk_market_id WHERE b.id IS NOT NULL AND s.id IS NOT NULL AND m.id IS NOT NULL order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = ''
	i = 0
	total = cursor.rowcount

	for row in records:
		bookmaker_title = row[1]
		if not os.path.exists('market_parser/markets/' + bookmaker_title):
			os.makedirs('market_parser/markets/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('market_parser/markets/' + current_bookmaker + '/rules.csv'):
				os.remove('market_parser/markets/' + current_bookmaker + '/rules.csv')

			csv_path = 'market_parser/markets/' + current_bookmaker + '/rules.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[2]) + '@s.s@' + row[3] + '@s.s@' + str(row[4]) + '@s.s@' + row[5] + '@s.s@' + row[6] + '@s.s@' + (row[7] if row[7] else 'None') + '@s.s@' + row[8] + '\n'
		i += 1

	# Outcome market rules
	cursor.execute("select b.id as bookmaker_id, b.title as bookmaker_title, m.id as market_id, m.title as market_title, s.id as sport_id, s.title as sport_title, bmor.id, bmor.input, bmor.input_replace, mo.name as market_outcome_name, mo.save_pattern as market_outcome_save_pattern, mo.output as market_outcome_output from bookmaker_market_outcome_rules bmor LEFT JOIN bookmakers b ON b.id = bmor.fk_bookmaker_id LEFT JOIN markets m ON m.id = bmor.fk_market_id LEFT JOIN sports s ON s.id = m.fk_sport_id LEFT JOIN market_outcomes mo ON mo.id = bmor.fk_market_outcome_id where b.id is not null and m.id is not null order by b.id")
	records = cursor.fetchall()
	current_bookmaker = None
	csv_rows = ''
	i = 0
	total = cursor.rowcount

	for row in records:
		bookmaker_title = row[1]
		if not os.path.exists('market_parser/outcomes/' + bookmaker_title):
			os.makedirs('market_parser/outcomes/' + bookmaker_title)
			
		if not current_bookmaker:
			current_bookmaker = bookmaker_title
		elif current_bookmaker != bookmaker_title or i == total - 1:

			if os.path.exists('market_parser/outcomes/' + current_bookmaker + '/rules.csv'):
				os.remove('market_parser/outcomes/' + current_bookmaker + '/rules.csv')

			csv_path = 'market_parser/outcomes/' + current_bookmaker + '/rules.csv'
			with open(csv_path, 'w', encoding="utf-8") as fd:
				fd.write(csv_rows)

			current_bookmaker = bookmaker_title
			csv_rows = ''

		csv_rows += str(row[2]) + '@s.s@' + row[3] + '@s.s@' + str(row[4]) + '@s.s@' + row[5]  + '@s.s@' + str(row[6]) + '@s.s@' + row[7] + '@s.s@' + (row[8] if row[8] else 'None') + '@s.s@' + row[9] + '@s.s@' + row[10] + '@s.s@' + row[11] + '\n'
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