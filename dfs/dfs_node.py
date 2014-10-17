import zmq
import time
import sys
import os
import pickle

storage_path = "/home/art-vybor/dfs_storage"

if not os.path.exists(storage_path):
    os.makedirs(storage_path)


def get_data(index):
    with open('{STORAGE}/{INDEX}'.format(STORAGE=storage_path, INDEX=str(index)), 'r') as file:
        return file.read()

def write_data(index, data):
    with open('{STORAGE}/{INDEX}'.format(STORAGE=storage_path, INDEX=str(index)), 'w') as file:
        file.write(data)


socket = zmq.Context().socket(zmq.REP)
socket.bind("tcp://*:5556")

while True:

    message = socket.recv()
    try:
        message = pickle.loads(message)
        if 'read' in message:
            index = message['read']
            data = get_data(index)
            socket.send(data)
        elif 'write' in message:
            index, data = message['write']
            write_data(index, data)
            socket.send('ok')
    except:
        pass