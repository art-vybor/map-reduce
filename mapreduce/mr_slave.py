import argparse
import zmq
from time import time
import sys
import os
import marshal
from lib.dfs_lib import split, read_block, write_block
from lib.block_manager import block_manager
import operator
import heapq

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5557')

    return parser.parse_args()

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
        block = read(index, dfs_port)

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

def main():
    args = parse_args()
    port = args.port

    socket = zmq.Context().socket(zmq.REP)
    socket.bind("tcp://*:{PORT}".format(PORT=port))

    while True:
        message = socket.recv()
        try:
            message = marshal.loads(message)
            dfs_port = message['dfs_port']                

            if 'map' in message:
                indexes, mr_file = message['map']                
                print 'map: {INDEXES}'.format(INDEXES=indexes)

                start = time()
                data = do_map_functions(indexes, mr_file, dfs_port)
                print 'map finished: %ss' % (time() - start)

                start = time()
                data = marshal.dumps(data)
                print 'serialize finished: %ss' % (time() - start)

                socket.send(data)
            elif 'reduce' in message:
                pairs = message['reduce']
                print 'reduce: {NUM} pairs'.format(NUM=len(pairs))

                start = time()
                indexes = do_reduce_functions(pairs, dfs_port)
                print 'reduce finished: %ss' % (time() - start)
                
                socket.send(marshal.dumps((indexes, message['node'])))

        except Exception as inst:
            print 'ERROR %s' % inst
            

if __name__ == "__main__":
    main()
