import ijson
import requests
import time
import os

start_time = time.time()

# Loop bookmakers
bookmakers = ijson.items(open('../bookmakers.json', 'r'), 'item');

for bookmaker in bookmakers:
	bookmaker_title = bookmaker.get('title')
	update = bookmaker.get('update')

	if update:
		print('Running download script for ' + bookmaker_title + ': ' + str(update))

		if update and os.path.exists('Downloaders/' + bookmaker_title):
			os.system('cd Downloaders/' + bookmaker_title + ' && run.py')

print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))