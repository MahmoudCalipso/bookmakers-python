import ijson
import requests
import time
import os
import sys
import csv
from datetime import date
import bookmaker_consumer

reader = 'Generic'
start_time = time.time()

if len(sys.argv) > 1:
	reader = sys.argv[1]

print('Initiating ' + str(reader) + ' reader...')
print('-------------------------------------------------------------------')

# Loop bookmakers
bookmakers = ijson.items(open('../../bookmakers.json', 'r'), 'item');

for bookmaker in bookmakers:
	if bookmaker.get('seeder') == reader or reader == 'Generic':
		bookmaker_id = bookmaker.get('id')
		bookmaker_title = bookmaker.get('title')

		print('Running seeder for ' + bookmaker_title)
		bookmaker_consumer.run(bookmaker_id, bookmaker_title)			

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))