import zmq
import pickle
import os


class block_manager:
    def __init__(self, config):
        self.bm_file = os.path.join(os.path.dirname(__file__), 'etc/bm.pickle')
        self.dfs_nodes = config['nodes']

        self.index_map = pickle.load(open(self.bm_file, 'rb')) if os.path.exists(self.bm_file) else {}

        if 0 not in self.index_map:
            self.index_map[0] = 1

        for dfs_node in self.dfs_nodes:
            dfs_node['socket'] = zmq.Context().socket(zmq.REQ)
            dfs_node['socket'].connect('{URL}:{PORT}'.format(URL=dfs_node['url'], PORT=dfs_node['dfs_port']))

    def get_index(self):
        index = self.index_map[0]
        self.index_map[0] += 1
        return index

    def split_indexes_by_node(self, indexes):
        result = {}
        for index in indexes:
            node = self.index_map[index]
            result[node] = result[node] + index if node in result else [index]

        return result

    def save(self):
        pickle.dump(self.index_map, open(self.bm_file, 'wb'))

    def send(func_word, data, index):
        request = {func_word: data}
        dfs_node = self.dfs_nodes[self.index_map[index]]
        dfs_node['socket'].send(pickle.dumps(request))
        return dfs_node['socket'].recv()

    def read(self, index):
        return send('read', index, index)

    def write(self, index, data, dfs_node):
        return send('remove', (index, data), index) == 'ok' 

    def remove(self, index):
        return send('remove', index, index) == 'ok'

    def remove_blocks(self, indexes):
        for index in indexes:
            if not self.remove(index):
                raise Exception("Block with index {INDEX} based of server {SERVER} can not be removed".
                    format(INDEX=index, SERVER=self.dfs_nodes[self.index_map[index]]['url']))

    def put_blocks(self, blocks):
        dfs_node_index = 0;
        indexes = []

        for block in blocks:
            index = self.get_index()

            if self.write(index, block, self.dfs_nodes[dfs_node_index]):
                self.index_map[index] = dfs_node_index            
                indexes.append(index)
            else:
                raise Exception("Can't write block to {SERVER}".
                    format(SERVER=self.dfs_nodes[dfs_node_index]['url']))
            dfs_node_index += 1

            if dfs_node_index == len(self.dfs_nodes):
                dfs_node_index = 0

        return indexes

    def read_blocks(self, indexes):
        blocks = []
        for index in indexes:
            yield self.read(index)