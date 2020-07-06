# import socket programming library 
import socket
import json
import os
import sys
import traceback
from threading import Thread

def clientThread(connection, ip, port, max_buffer_size = 5120):
	is_active = True
	while is_active:
		data = receive_input(connection, max_buffer_size)

		if not data:               
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

def receive_input(connection, max_buffer_size):
	client_input = connection.recv(max_buffer_size)
	client_input_size = sys.getsizeof(client_input)

	if client_input_size > max_buffer_size:
		print("The input size is greater than expected {}".format(client_input_size))
	
	return client_input.decode("utf8").rstrip()

def Main(): 
	host = "" 
  
	# reverse a port on your computer 
	# in our case it is 12345 but it 
	# can be anything 
	port = 12345
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	print("Socket created")
	try:
		s.bind((host, port)) 
	except:
		print('Bind failed. Error: ' + str(sys.exc_info()))
		sys.exit()

	print("socket binded to port", port)
	# put the socket into listening mode 
	s.listen(20)
	print("socket is listening")
	
	# a forever loop until client wants to exit 
	while True: 
		# establish connection with client 
		connection, address = s.accept() 
		ip, port = str(address[0]), str(address[1])
		print("Connected with " + ip + ":" + port)
		try:
			Thread(target=clientThread, args=(connection, ip, port)).start()
		except:
			print("Thread did not start.")
			traceback.print_exc()
			s.close()
  
if __name__ == '__main__': 
	Main()