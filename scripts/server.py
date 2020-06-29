import socketio
import uvicorn
import os

# create a Socket.IO server
sio = socketio.Server(async_mode='threading')
app = socketio.WSGIApp(sio)

@sio.event
def connect(sid, environ):
	print('connect ', sid)

@sio.event
def disconnect(sid):
	print('disconnect ', sid)

@sio.on('download_complete', namespace='/readers')
def download_complete(sid, data):
	if data['bookmaker']:
		print('Download complete! Waking up reader...')
		os.system('cd Readers/' + data['bookmaker'] + ' && python run.py')
	pass

@sio.on('read_complete', namespace='/seeder')
def read_complete(sid, data):
	if data['bookmaker_id'] and data['bookmaker_title']:
		print('Download complete! Waking up ' + data['bookmaker_title'] + ' seeder...')
		os.system('cd Seeder && python run.py ' + str(data['bookmaker_id']) + ' ' + data['bookmaker_title'])
	pass

if __name__ == '__main__':
	uvicorn.run(app, host='127.0.0.1', port=5000)