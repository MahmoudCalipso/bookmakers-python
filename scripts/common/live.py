# Reload data used by bookmaker updater and consumer

import psycopg2
import os
import time
import csv
from datetime import datetime, timedelta

start_time = time.time()

try:
	connection = psycopg2.connect(user = "asanchez",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet")

	cursor = connection.cursor()

	# Update all the events that are equal or before this date
	now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

	try:
		cursor.execute("UPDATE events e SET live = 1 WHERE e.live = 0 AND CONCAT(e.date, \' \', e.time) <= '" + now + "'")
		connection.commit()
		print("Affected rows = {}".format(cursor.rowcount))
	except (Exception, psycopg2.Error) as error:
		connection.rollback()
		print ("Error while updating live events", error)

	try:
		cursor.execute(
			"UPDATE events e SET has_markets = 1 WHERE e.has_markets = 0 AND e.id IN (" +
				"SELECT e.id FROM bookmaker_event_market_outcomes bemo " +
				"LEFT JOIN bookmaker_event_markets bem ON bem.id = bemo.fk_bookmaker_event_market_id " +
				"LEFT JOIN bookmaker_events be ON be.id = bem.fk_bookmaker_event " +
				"LEFT JOIN events e ON e.id = be.fk_event_id " +
				"GROUP BY e.id " +
				"HAVING COUNT(bemo.*) > 0" +
			")"
		)
		connection.commit()
		print("Affected rows = {}".format(cursor.rowcount))
	except (Exception, psycopg2.Error) as error:
		connection.rollback()
		print ("Error while updating has_markets flag", error)
except (Exception, psycopg2.Error) as error:
	if connection:
		connection.rollback()
	print ("Error while connecting to PostgreSQL", error)
finally:
	#closing database connection.
		if(connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))