import zmq
import pickle
import os
import zlib

from copy import deepcopy
from dfs_lib import send, read_block, write_block, remove_block

class block_manager:
    def __init__(self, config=None):
        
        self.dfs_nodes = deepcopy(config['nodes'])
        for dfs_node in self.dfs_nodes:
            dfs_node['socket'] = zmq.Context().socket(zmq.REQ)
            dfs_node['socket'].connect('{URL}:{PORT}'.format(URL=dfs_node['url'], PORT=dfs_node['dfs_port']))

        self.bm_file = os.path.join(os.path.dirname(__file__), '../etc/bm.pickle')
        self.index_map = pickle.load(open(self.bm_file, 'rb')) if os.path.exists(self.bm_file) else {}

        if 0 not in self.index_map:
            self.index_map[0] = 1

    def save(self):
        pickle.dump(self.index_map, open(self.bm_file, 'wb'))

    def get_index(self):
        index = self.index_map[0]
        self.index_map[0] += 1
        return index

    def get_socket(self, index):
        return self.dfs_nodes[self.index_map[index]]['socket']

    def split_indexes_by_node(self, indexes):
        result = {}
        for index in indexes:
            node = self.index_map[index]
            if node not in result:
                result[node] = []
            result[node].append(index)

        return result

    def change_block_index(self, old_index, new_index, dfs_node_index):
        socket = self.dfs_nodes[dfs_node_index]['socket']
        return send(socket, 'change_index', (old_index, new_index)) == 'ok'

    def read_blocks(self, indexes):
        blocks = []
        for index in indexes:
            yield zlib.decompress(read_block(self.get_socket(index), index))

    def write_blocks(self, blocks):
        dfs_node_index = 0;
        indexes = []

        for block in blocks:
            index = self.get_index()

            if write_block(self.dfs_nodes[dfs_node_index]['socket'], index, zlib.compress(block)):
                self.index_map[index] = dfs_node_index            
                indexes.append(index)
            else:
                raise Exception("Can't write block to {SERVER}".
                    format(SERVER=self.dfs_nodes[dfs_node_index]['url']))
            dfs_node_index += 1

            if dfs_node_index == len(self.dfs_nodes):
                dfs_node_index = 0

        return indexes

    def remove_blocks(self, indexes):
        for index in indexes:
            if not remove_block(self.get_socket(index), index):
                raise Exception("Block with index {INDEX} based of server {SERVER} can not be removed".
                    format(INDEX=index, SERVER=self.dfs_nodes[self.index_map[index]]['url']))