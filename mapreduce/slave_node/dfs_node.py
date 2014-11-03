import argparse
import zmq
import time
import sys
import os
import pickle

def parse_args():
    parser = argparse.ArgumentParser(description='Dfs node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5556')

    parser.add_argument('-s', help='absolute path to storage',
        metavar='path', dest='path', default='/home/avybornov/dfs_storage')
    
    return parser.parse_args()

args = parse_args()
storage_path = args.path
port = args.port

def get_filepath(index):
    return os.path.join(storage_path, str(index))

def read_data(index): 
    with open(get_filepath(index), 'r') as file:
        return file.read()

def write_data(index, data):
    with open(get_filepath(index), 'w') as file:
        file.write(data)

def remove_data(index):
    os.remove(get_filepath(index))

def main():
    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    socket = zmq.Context().socket(zmq.REP)
    socket.bind("tcp://*:{PORT}".format(PORT=port))

    while True:
        message = socket.recv()
        try:
            message = pickle.loads(message)
            if 'read' in message:
                index = message['read']
                print 'read: {INDEX}'.format(INDEX=index)
                data = read_data(index)
                socket.send(data)
            elif 'write' in message:
                index, data = message['write']
                print 'write: {INDEX}'.format(INDEX=index)
                write_data(index, data)
                socket.send('ok')
            elif 'remove' in message:
                index = message['remove']
                print 'remove: {INDEX}'.format(INDEX=index)                
                remove_data(index)
                socket.send('ok')
        except:
            pass

if __name__ == "__main__":
    main()