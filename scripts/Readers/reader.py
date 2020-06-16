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
print('Reading: ' + queue_csv_path)
print('-------------------------------------------------------------------')

# Extract row from CSV and process it
if os.path.exists(queue_csv_path):
	with open(queue_csv_path, 'r', newline='') as file:
		reader = csv.reader(file)
		for row in reader:
			print(row)


print("--- The whole process took %s seconds to complete ---" % (time.time() - start_time))