import ijson
import requests
import time
import os
import sys

start_time = time.time()
is_live = False

if len(sys.argv) > 1 and sys.argv[1] == 'live':
	is_live = True

download_type = 'live' if is_live else 'prematch'

# Loop bookmakers
bookmakers = ijson.items(open('../bookmakers.json', 'r'), 'item');

for bookmaker in bookmakers:
	bookmaker_title = bookmaker.get('title')
	update_config = bookmaker.get('update')
	update = update_config.get(download_type)

	if update:
		print('-------------------------------------------------------------------')
		print('Running ' + ('live' if is_live else 'prematch') + ' download for ' + bookmaker_title + ': ' + str(update))

		if update and os.path.exists('Downloaders/' + bookmaker_title):
			os.system('cd Downloaders/' + bookmaker_title + ' && python run.py' + (' live' if is_live else ''))
			print('-------------------------------------------------------------------')

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))