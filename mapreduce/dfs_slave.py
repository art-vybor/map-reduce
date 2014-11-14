import argparse
import zmq
import os
import marshal

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
                print 'read {INDEX}'.format(INDEX=index)
                data = read_block(storage_path, index)
                socket.send(data)
            elif 'write' in message:
                index, data = message['write']
                print 'write {INDEX}'.format(INDEX=index)
                write_block(storage_path, index, data)
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
        except:
            pass

if __name__ == "__main__":
    main()