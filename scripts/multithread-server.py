# import socket programming library 
import socket
import json
import os
  
# import thread module 
from _thread import *
import threading 
  
print_lock = threading.Lock() 
  
# thread function 
def threaded(c): 
	while True: 
  
		# data received from client 
		data = c.recv(1024).decode('utf8')
		
		if not data:               
			# lock released on exit 
			print_lock.release() 
			break

		data = json.loads(data)
		print('Message received from ' + data['data']['bookmaker_title'] + ': ' + data['message'])

		if data['message'] == 'download_complete':
			data = data['data']
			if 'bookmaker_title' in data:
				print('Download complete! Waking up ' + data['bookmaker_title'] + ' reader...')
				os.system('cd Readers/' + data['bookmaker_title'] + ' && python run.py ' + data['type'] + ' ' + data['timestamp'] + ' ' + data['started_at']) 
		elif data['message'] == 'read_complete':
			data = data['data']
			# run reader
			if 'bookmaker_id' in data and 'bookmaker_title' in data:
				print('Download complete! Waking up ' + data['bookmaker_title'] + ' seeder...')
				os.system('cd Seeder && python run.py ' + str(data['bookmaker_id']) + ' ' + data['bookmaker_title'] + ' ' + data['timestamp'] + ' ' + str(data['live']) + ' ' + data['started_at'])
  
	# connection closed 
	c.close()
  
def Main(): 
	host = "" 
  
	# reverse a port on your computer 
	# in our case it is 12345 but it 
	# can be anything 
	port = 12345
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((host, port)) 
	print("socket binded to port", port) 
  
	# put the socket into listening mode 
	s.listen(5) 
	print("socket is listening") 
  
	# a forever loop until client wants to exit 
	while True: 
  
		# establish connection with client 
		c, addr = s.accept() 
  
		# lock acquired by client 
		print_lock.acquire() 
		print('Connected to :', addr[0], ':', addr[1]) 
  
		# Start a new thread and return its identifier 
		start_new_thread(threaded, (c,)) 
	s.close() 
  
if __name__ == '__main__': 
	Main()