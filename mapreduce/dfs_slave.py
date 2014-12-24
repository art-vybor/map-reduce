import argparse
import zmq
import os
import marshal
import zlib

def parse_args():
    parser = argparse.ArgumentParser(description='Dfs node.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', help='port',
        metavar='port', dest='port', default='5556')

    parser.add_argument('-s', help='absolute path to storage',
        metavar='path', dest='path', default='/home/avybornov/dfs_storage')
    
    return parser.parse_args()

def convert_index(index):
    return str(index).replace('-', '_')

def get_filepath(storage_path, index):
    return os.path.join(storage_path, str(index))

def read_block(storage_path, index): 
    with open(get_filepath(storage_path, index), 'r') as file:
        return file.read()

def write_block(storage_path, index, block):    
    with open(get_filepath(storage_path, convert_index(index)), 'w') as file:
        file.write(block)

def remove_block(storage_path, index):
    os.remove(get_filepath(storage_path, index))

def change_block_index(storage_path, old_index, new_index):
    old_path = get_filepath(storage_path, convert_index(old_index))
    new_path = get_filepath(storage_path, convert_index(new_index))
    os.rename(old_path, new_path)

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
            message = marshal.loads(message)
            if 'read' in message:
                index = message['read']
                compress = message['compress']

                print 'read {INDEX}'.format(INDEX=index)
                block = read_block(storage_path, index)
                if compress:
                    block = zlib.compress(block, 1)
                socket.send(block)
            elif 'write' in message:
                index, block = message['write']
                compress = message['compress']

                print 'write {INDEX}'.format(INDEX=index)

                if compress:
                    block = zlib.decompress(block)
                write_block(storage_path, index, block)
                socket.send('ok')
            elif 'remove' in message:
                index = message['remove']
                print 'remove {INDEX}'.format(INDEX=index)                
                remove_block(storage_path, index)
                socket.send('ok')
            elif 'change_index' in message:
                (old_index, new_index) = message['change_index']
                print 'change_index {OLD} to {NEW}'.format(OLD=old_index, NEW=new_index)
                change_block_index(storage_path, old_index, new_index)
                socket.send('ok')
        except Exception as inst:
            print 'ERROR: %s' % inst


if __name__ == "__main__":
    main()