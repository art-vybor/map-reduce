import argparse
import zmq
from time import time
import sys
import os
import cPickle as pickle
from lib.dfs_lib import split, read_block, write_block
from lib.block_manager import block_manager
import operator
from mr import merge
import heapq

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

def union_by_key(pairs):
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

def get_socket(port):
    socket = zmq.Context().socket(zmq.REQ)
    socket.connect('tcp://localhost:{PORT}'.format(PORT=port))
    return socket

def read(index, dfs_port):
    return read_block(get_socket(dfs_port), index)

def write(index, block, dfs_port):
    return write_block(get_socket(dfs_port), index, block)

def do_map_functions(indexes, mr_file, dfs_port):
    with open('mr_file.py', 'w') as f:
        f.write(mr_file)

    from mr_file import map_func
    
    result_dict = {}

    for index in indexes:
        with open(os.path.join('/home/avybornov/dfs_storage', str(index)),'r') as block_file:
            print 'start %s' % index
            block = block_file.read()

            for x in map_func(block):
                if x[0] in result_dict:
                    result_dict[x[0]].append(x[1])
                else:
                    result_dict[x[0]] = [x[1]]

    return result_dict 

def do_reduce_functions(pairs, dfs_port):
    from mr_file import reduce_func

    pairs = [reduce_func(*pair) for pair in pairs]

    blocks = split(pairs, lambda x: '%s %s\n' % (x))
    indexes = []

    index = -1

    for block in blocks:
        if write(index, block, dfs_port):                           
            indexes.append(index)
            index -= 1
        else:
             raise Exception("Can't write block with index {INDEX}".
                    format(INDEX=index))

    return indexes

def merge(pairs_list):
    def append_to_result(res, el):
        if len(res) > 0 and res[len(res)-1][0] == el[0]:
            res[len(res)-1][1].append(el[1])
        else:
            res.append([el[0], [el[1]]])     

    result = []

    while len(pairs_list) > 0:
        min_idx = 0

        for i in range(1, len(pairs_list)):
            if pairs_list[i][0] < pairs_list[min_idx][0]:
                min_idx = i

        append_to_result(result, pairs_list[min_idx][0])
        
        pairs_list[min_idx] = pairs_list[min_idx][1:len(pairs_list[min_idx])]
        if len(pairs_list[min_idx]) == 0:
            pairs_list.pop(min_idx)
        
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
            dfs_port = message['dfs_port']                

            if 'map' in message:
                indexes, mr_file = message['map']                
                print 'map: {INDEXES}'.format(INDEXES=indexes)
                start = time()
                data = do_map_functions(indexes, mr_file, dfs_port)
                print 'map finished: %ss' % (time() - start)

                print 'pickle started'
                start = time()
                data = pickle.dumps(data)

                print 'pickle finished: %ss' % (time() - start)
                socket.send(data)
            elif 'reduce' in message:
                pairs = message['reduce']
                print 'reduce: {NUM} pairs'.format(NUM=len(pairs))
                indexes = do_reduce_functions(pairs, dfs_port)
                socket.send(pickle.dumps((indexes, message['node'])))

        except Exception as inst:
            print 'ERROR %s' % inst
            

if __name__ == "__main__":
    main()
