import ijson
import requests
import time
import os
import sys
import csv
from datetime import date
import bookmaker_consumer

if len(sys.argv) > 5:
	start_time = time.time()

	bookmaker_id = sys.argv[1]
	bookmaker_title = sys.argv[2]
	timestamp = sys.argv[3]
	live = sys.argv[4]
	started_at = sys.argv[5]

	print('Reading ' + bookmaker_title + ' queue (' + bookmaker_id + ')')
	print('-------------------------------------------------------------------')

	# Loop bookmakers
	bookmaker_consumer.run(bookmaker_id, bookmaker_title, timestamp, live, started_at)

	print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))