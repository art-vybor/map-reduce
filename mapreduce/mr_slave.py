from __future__ import with_statement
import argparse
import threading
import zmq
import json
import os
import marshal

from time import time
from lib.dfs_lib import split, read_block, write_block
from lib.key_value_pool import key_value_pool


kv_buffer = {}

def parse_config(config_path):
     with open(config_path) as config_file:
         config_json = json.load(config_file)
         return config_json

config_path = os.path.join(os.path.dirname(__file__), 'etc/config.json')
config = parse_config(config_path)

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p1', help='port used by mr.py',
        metavar='port mr', dest='port_mr', default='5557')

    parser.add_argument('-p2', help='port user by mr_slave.py',
        metavar='port mr_slave', dest='port_mr_slave', default='5558')

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
    import mr_file as mr    
    reload(mr)

    kv_pool = key_value_pool(config)

    for index in indexes:
        block = read(index, dfs_port)

        kv_dict = {}

        for row in block.split('\n'):
            if len(row) > 0:
                for key, value in mr.map_func(row):
                    
                    if key in kv_dict:
                        kv_dict[key].append(value)
                    else:
                        kv_dict[key] = [value]            
        kv_pool.add_dict(kv_dict)

def do_reduce_functions(pairs, dfs_port):
    import mr_file as mr
    res_pairs = []

    for pair in pairs.iteritems():
        res_pairs.append(mr.reduce_func(*pair))
    blocks = split(res_pairs, lambda x: '%s %s\n' % (x[0], x[1]))
    indexes = []

    index = -1
    for block in blocks:
        if write(index, block, dfs_port):                           
            indexes.append(index)
            index -= 1
        else:
            raise Exception("Can't write block with index {INDEX}".
                    format(INDEX=index))
    print 5
    return indexes 

def mr_extend(d1, d2):
    for k, v in d2.iteritems():
        if k in d1:
            d1[k].extend(v)
        else:
            d1[k] = v

def map_reduce_server(port):
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

                with open('mr_file.py', 'w') as f: #TODO replace dest dir
                    f.write(mr_file)

                do_map_functions(indexes, mr_file, dfs_port)                
                print 'map finished: %ss' % (time() - start)

                socket.send(marshal.dumps('ok'))

            elif 'reduce' in message:
                global kv_buffer

                print 'reduce: {NUM} pairs'.format(NUM=len(kv_buffer))
                start = time()
                
                indexes = do_reduce_functions(kv_buffer, dfs_port)

                kv_buffer = {}
                print 'reduce finished: %ss' % (time() - start)

                socket.send(marshal.dumps((indexes, message['node'])))
            else:
                print 'incorrect request'

        except Exception as inst:
            print 'ERROR (map_reduce): %s' % inst

lock = threading.Lock()

def add_pairs_server(port):
    global lock
    socket = zmq.Context().socket(zmq.REP)
    socket.bind("tcp://*:{PORT}".format(PORT=port))

    while True:
        message = socket.recv()
        try:
            message = marshal.loads(message)
            dfs_port = message['dfs_port']                

            if 'add_pairs' in message:
                global kv_buffer
                
                kv_dict = message['add_pairs']
                print '\tadd_pairs: {NUM} pairs to add'.format(NUM=len(kv_dict))

                with lock:
                    mr_extend(kv_buffer, kv_dict)

                socket.send(marshal.dumps('ok'))
            else:
                print '\tincorrect request'

        except Exception as inst:
            print 'ERROR (add_pairs): %s' % inst

def main():
    args = parse_args()
    port_mr = args.port_mr
    port_mr_slave = args.port_mr_slave

    threading.Thread(target = map_reduce_server, args = (port_mr, )).start()
    threading.Thread(target = add_pairs_server, args = (port_mr_slave, )).start()

if __name__ == "__main__":
    main()
