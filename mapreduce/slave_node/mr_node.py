import argparse
import zmq
import time
import sys
import os
import pickle
from dfs_node import read_data
import operator

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5557')

    return parser.parse_args()

def sort_and_union_by_key(pairs):
    pairs = sorted(pairs) #it's too bad

    result_pairs = []

    new_pair = (pairs[0][0], [])
    for pair in pairs:
        if pair[0] == new_pair[0]:
            new_pair[1].append(pair[1])
        else:
            result_pairs.append(new_pair)
            new_pair = (pair[0], [pair[1]])

    result_pairs.append(new_pair)

    return result_pairs

def do_map_functions(indexes, mr_file):
    with open('mr_file.py', 'w') as f:
        f.write(mr_file)

    from mr_file import map_func
    
    result = []
    for index in indexes:
        data = read_data(index)
        for pair in map_func(data):
            result.append(pair)
        result = sort_and_union_by_key(result)

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
