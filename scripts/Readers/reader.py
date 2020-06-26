import ijson
import requests
import time
import os
import sys
import csv
from datetime import date

reader = 'Generic'
start_time = time.time()
queue_path = '../../queues/Downloaders/'
queue_csv_path = queue_path + 'queue_' + date.today().strftime("%d-%m-%Y") + '.csv';

if len(sys.argv) > 1:
	reader = sys.argv[1]

print('Initiating ' + str(reader) + ' reader...')
print('-------------------------------------------------------------------')

# Loop bookmakers
bookmakers = ijson.items(open('../../bookmakers.json', 'r'), 'item');

for bookmaker in bookmakers:
	if bookmaker.get('reader') == reader or reader == 'Generic':
		bookmaker_title = bookmaker.get('title')
		update_config = bookmaker.get('update')
		update = update_config.get(download_type)

		if update:
			print('Running ' + ('live' if is_live else 'prematch') + ' reader for ' + bookmaker_title + ': ' + str(update))

			if update and os.path.exists(bookmaker_title):
				os.system('cd ' + bookmaker_title + ' && python run.py' + (' live' if is_live else ''))
				print('-------------------------------------------------------------------')


print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))