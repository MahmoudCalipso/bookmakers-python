import ijson
import requests
import time
import os
import sys

downloader = 'Generic'
start_time = time.time()
is_live = False

if len(sys.argv) > 1:
	downloader = sys.argv[1]

if len(sys.argv) > 2 and sys.argv[2] == 'live':
		is_live = True

download_type = 'live' if is_live else 'prematch'

print('Initiating ' + str(downloader) + ' downloader...')
print('-------------------------------------------------------------------')

# Loop bookmakers
bookmakers = ijson.items(open('../../bookmakers.json', 'r'), 'item');

for bookmaker in bookmakers:
	if bookmaker.get('downloader') == downloader or downloader == 'Generic':
		bookmaker_title = bookmaker.get('title')
		update_config = bookmaker.get('update')
		update = update_config.get(download_type)

		if update:
			print('Running ' + ('live' if is_live else 'prematch') + ' download for ' + bookmaker_title + ': ' + str(update))

			if update and os.path.exists(bookmaker_title):
				os.system('cd ' + bookmaker_title + ' && python run.py' + (' live' if is_live else ''))
				print('-------------------------------------------------------------------')

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))