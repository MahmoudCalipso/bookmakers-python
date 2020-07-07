# Reload data used by bookmaker updater and consumer

import psycopg2
import os
import time
import csv
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

start_time = time.time()

try:
	connection = psycopg2.connect(user = "asanchez",
								  password = "aegha5Cu",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "scannerbet")

	cursor = connection.cursor()

	# Subtract two days
	now = datetime.now() - timedelta(days=1)
	now = now.replace(hour=23, minute=59, second=59)
	two_days_ago = datetime.now() - timedelta(days=2)
	two_days_ago = now.replace(hour=0, minute=0, second=0)

	print('Deleting update queues')
	try:
		cursor.execute("DELETE FROM bookmaker_update_queues buq WHERE buq.created_at <= '" + now.strftime('%Y-%m-%d %H:%M:%S') + "'")
		connection.commit()
		print("Affected rows = {}".format(cursor.rowcount))
	except (Exception, psycopg2.Error) as error:
		connection.rollback()
		print("Error while deleting update queues", error)

	# Delete all the outcomes belonging to those events that are equal or previous to this date
	print('Deleting events')
	try:
		now = now - relativedelta(months=3)
		cursor.execute("DELETE FROM events e WHERE CONCAT(e.date, \' \', e.time) <= '" + now.strftime('%Y-%m-%d %H:%M:%S') + "'")
		cursor.execute(
			"DELETE FROM bookmaker_events be where fk_event_id IN (" +
				"SELECT id FROM events e " +
				"WHERE e.related_to_market IS NULL AND CONCAT(e.date, \' \', e.time) <= '" + now.strftime('%Y-%m-%d %H:%M:%S') + "' "
			")"
		)
		connection.commit()
		print("Affected rows = {}".format(cursor.rowcount))
	except (Exception, psycopg2.Error) as error:
		connection.rollback()
		print("Error while deleting events", error)

	# Delete all outcomes from outright markets that haven't received an update for 2 days
	print('Deleting all outcomes from outright markets that haven\'t received an update for 2 days')
	try:
		cursor.execute(
			"DELETE FROM bookmaker_event_market_outcomes bemo where fk_bookmaker_event_market_id IN (" +
				"SELECT bem.id FROM bookmaker_event_markets bem " +
				"LEFT JOIN bookmaker_events be ON be.id = bem.fk_bookmaker_event " +
				"LEFT JOIN events e ON e.id = be.fk_event_id " +
				"WHERE e.related_to_market IS NOT NULL " +
				"GROUP BY bem.id" +
			") AND bemo.updated_at <= '" + two_days_ago.strftime('%Y-%m-%d %H:%M:%S') + "'"
		)
		connection.commit()
		print("Affected rows = {}".format(cursor.rowcount))
	except (Exception, psycopg2.Error) as error:
		connection.rollback()
		print ("Error while deleting outright outcomes", error)
	
except (Exception, psycopg2.Error) as error:
	if connection:
		connection.rollback()
	print("Error while connecting to PostgreSQL", error)
finally:
	#closing database connection.
		if(connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))