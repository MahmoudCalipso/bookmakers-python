import socketio
import uvicorn
import os

# create a Socket.IO server
sio = socketio.AsyncServer(async_mode='asgi')
app = socketio.ASGIApp(sio)

@sio.event
def connect(sid, environ):
	print('connect ', sid)

@sio.event
def disconnect(sid):
	print('disconnect ', sid)

@sio.on('download_complete', namespace='/readers')
def my_custom_event(sid, data):
	if data['bookmaker']:
		print('Download complete! Waking up reader...')
		#os.system('cd Readers/' + data['bookmaker'] + ' && python run.py')
	pass

if __name__ == '__main__':
	uvicorn.run(app, host='127.0.0.1', port=5000)