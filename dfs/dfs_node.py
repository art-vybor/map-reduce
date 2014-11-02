import argparse
import zmq
import time
import sys
import os
import pickle

def read_data(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def write_data(file_path, data):
    with open(file_path, 'w') as file:
        file.write(data)

def remove_data(file_path):
    os.remove(file_path)


def parse_args():
    parser = argparse.ArgumentParser(description='Dfs node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5556')
    
    parser.add_argument('-s', help='absolute path to storage',
        metavar='path', dest='path', default='/home/avybornov/dfs_storage')
    
    return parser.parse_args()


def main():
    args = parse_args()
    storage_path = args.path
    port = args.port

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
                data = read_data(os.path.join(storage_path, str(index)))
                socket.send(data)
            elif 'write' in message:
                index, data = message['write']
                write_data(os.path.join(storage_path, str(index)), data)
                socket.send('ok')
            elif 'remove' in message:
                index = message['remove']
                remove_data(os.path.join(storage_path, str(index)))
                socket.send('ok')
        except:
            pass

if __name__ == "__main__":
    main()