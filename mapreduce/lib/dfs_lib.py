import zmq
import pickle

def send(socket, func_word, data):
    request = {func_word: data}
    socket.send(pickle.dumps(request))
    return socket.recv()

def read_block(socket, index):
    return send(socket, 'read', index)

def write_block(socket, index, block):
    return send(socket, 'write', (index, block)) == 'ok'

def remove_block(socket, index):
    return send(socket, 'remove', index) == 'ok' 

def split(elem_list, format_func, block_size_limit_mb=64):
    block_size_limit = block_size_limit_mb * 1024 * 1024

    block = []
    block_size = 0
    for elem in elem_list:
        to_append = format_func(elem)       
        block.append(to_append)
        block_size += len(to_append)

        if block_size > block_size_limit:            
            block_size = 0
            yield ''.join(block)
            block = []

    if block_size != 0:
        yield ''.join(block)