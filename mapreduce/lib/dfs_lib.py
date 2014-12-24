import zmq
import marshal
import zlib


def send(socket, func_word, data, compress=False):
    request = {func_word: data, 'compress': compress}
    socket.send(marshal.dumps(request))
    return socket.recv()

def read_block(socket, index, compress=False):
    response = send(socket, 'read', index, compress)
    if compress:
        response = zlib.decompress(response)

    return response

def write_block(socket, index, block, compress=False):
    if compress:
        block = zlib.compress(block, 1)
    return send(socket, 'write', (index, block), compress) == 'ok'

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