import argparse
import zmq
import time
import sys
import os
import pickle
from dfs_node import read_data

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5557')

    return parser.parse_args()

def do_map_functions(indexes, mr_file):
    with open('mr_file.py', 'w') as f:
        f.write(mr_file)

    from mr_file import map_func
    
    result = []
    for index in indexes:
        print 'index: %s' % index
        data = read_data(index)
        print data
        for x in map_func(data):
            print x
            result.append(x)

    return result    

def main():
    args = parse_args()
    port = args.port

    socket = zmq.Context().socket(zmq.REP)
    socket.bind("tcp://*:{PORT}".format(PORT=port))

    while True:
        message = socket.recv()
        try:
            message = pickle.loads(message)

            if 'map' in message:
                indexes, mr_file = message['map']
                print 'map: {INDEXES}'.format(INDEXES=indexes)                

                data = do_map_functions(indexes, mr_file)

                socket.send(pickle.dumps(data))
        except Exception as inst:
            print inst
            

if __name__ == "__main__":
    main()
