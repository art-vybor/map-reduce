import argparse
from lxml import etree
import pickle
import os
import zmq
import ast
import json

class DfsTree:
    def __init__(self, config):
        self.fs_file = config['fs_structure file']

        parser = etree.XMLParser(remove_blank_text=True)
        self.tree = etree.parse(self.fs_file, parser)

    def save(self):
        self.tree.write(self.fs_file, pretty_print=True)

    def is_folder(self, tree_element):
        return tree_element.get('index_list') is None

    def get_tree_element(self, path):
        tree_element = self.tree.xpath('/root' + path.rstrip('/'))

        if len(tree_element) == 1:
            return tree_element[0]
        else:
            raise Exception('Path {PATH} is incorrect'.format(PATH=path))

    def get_files_from_tree_element(self, tree_element):
        elementes = []

        if self.is_folder(tree_element):
            for element in tree_element:
                elementes.append(element)
        else:
            elementes.append(tree_element)

        return elementes

    def get_file_indexes(self, file):
        if not self.is_folder(file):
            return ast.literal_eval(file.get('index_list'))
        
        raise Exception('{FILE} is not a file.'.
                    format(FILE=file.tag))

    def get_recursive_files_indexes(self, tree_element):
        indexes = []

        if self.is_folder(tree_element):
            for element in self.get_files_from_tree_element(tree_element):
                indexes += self.get_recursive_files_indexes(element)
        else:
            indexes += self.get_file_indexes(tree_element)

        return indexes

    def mkdir(self, path):
        path_tokens = filter(lambda x: len(x) > 0, path.split('/'))
        tree_element = self.tree.getroot()

        for token in path_tokens:
            next_tree_element = tree_element.find(token)

            if next_tree_element is None:
                tree_element.append(etree.Element(token))
            elif not self.is_folder(next_tree_element):
                raise Exception('Path {PATH} does not be created: {TOKEN} is file.'.
                    format(PATH=path, TOKEN=token))

            tree_element = tree_element.find(token)

    def put(self, folder, name, index_list):
        if folder.find(name) is not None:
            raise Exception('File {PATH}/{NAME} already exists'.
                format(PATH=path.rstrip('/'), NAME=name))

        folder.append(etree.Element(name, index_list=str(index_list)))

    def ls(self, tree_element):
        ls = []
        for element in self.get_files_from_tree_element(tree_element):
            ls.append(element.tag + '/' if self.is_folder(element) else element.tag)

        return ls

    def rm(self, tree_element):
        tree_element.getparent().remove(tree_element)


class BlockManager:
    def __init__(self, config):
        self.bm_file = config['block_manager file']
        self.dfs_nodes = config['nodes']

        self.index_map = pickle.load(open(self.bm_file, 'rb')) if os.path.exists(self.bm_file) else {}

        if 0 not in self.index_map:
            self.index_map[0] = 1

        for dfs_node in self.dfs_nodes:
            dfs_node['socket'] = zmq.Context().socket(zmq.REQ)
            dfs_node['socket'].connect(dfs_node['url'])

    def get_index(self):
        index = self.index_map[0]
        self.index_map[0] += 1
        return index

    def save(self):
        pickle.dump(self.index_map, open(self.bm_file, 'wb'))

    def read(self, index):
        request = {'read': index}
        dfs_node = self.dfs_nodes[self.index_map[index]]
        dfs_node['socket'].send(pickle.dumps(request))
        return dfs_node['socket'].recv()

    def write(self, index, data, dfs_node):
        request = {'write': (index, data)}
        dfs_node['socket'].send(pickle.dumps(request))
        return dfs_node['socket'].recv() == 'ok'

    def remove(self, index):
        request = {'remove': index}
        dfs_node = self.dfs_nodes[self.index_map[index]]
        dfs_node['socket'].send(pickle.dumps(request))
        return dfs_node['socket'].recv() == 'ok'

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
            indexes.append(index)

            if self.write(index, '\n'.join(block), self.dfs_nodes[dfs_node_index]):
                self.index_map[index] = dfs_node_index            
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
            blocks.append(self.read(index))            
        return blocks

#n > 0
def split(input, n):
    rows = input.split('\n') #TODO rewrite for big file
    chunk_size = int(len(rows)/n) #TODO check python version and remove int()
    if chunk_size != 0:
        for i in xrange(0, n-1, chunk_size):
            yield rows[i:i+chunk_size]
        yield rows[chunk_size*(n-1):len(rows)] #TODO make more uniformly
    else:
        for i in xrange(0, n, 1):   
            yield rows[i:i+1] #slice should not be replaced by index


def parse_args():
    parser = argparse.ArgumentParser(description='Dfs command line interface.')

    parser.add_argument('-ls', help='list of directory',
        metavar='path', dest='ls_path')
    parser.add_argument('-mkdir', help='make new directory',
        metavar='path', dest='mkdir_path')
    parser.add_argument('-rm', help='remove file or folder',
        metavar='path', dest='rm_path')
    parser.add_argument('-put', nargs=2, help='put local file to dfs',
        metavar=('local_path', 'dfs_path'), dest='put_paths')
    parser.add_argument('-get', help='print file',
        metavar='path', dest='get_path')

    return parser.parse_args()

def parse_config(config_path='config.json'):
    with open(config_path) as config_file:
        config_json = json.load(config_file)
        return config_json

def main():
    args = parse_args()
    config = parse_config()

    dfs_tree = DfsTree(config)
    block_manager = BlockManager(config)

    if args.ls_path:
        tree_element = dfs_tree.get_tree_element(args.ls_path)
        print '\n'.join(dfs_tree.ls(tree_element))

    if args.mkdir_path:
        dfs_tree.mkdir(args.mkdir_path)

    if args.rm_path:
        tree_element = dfs_tree.get_tree_element(args.rm_path)
        indexes = dfs_tree.get_recursive_files_indexes(tree_element)
        block_manager.remove_blocks(indexes)
        dfs_tree.rm(tree_element)

    if args.put_paths:
        with open(args.put_paths[0], 'r') as input:            
            path = args.put_paths[1]
            if path.endswith('/'):
                raise Exception("Can't write file without name.")
            path, name = path.rsplit('/', 1)    
            folder = dfs_tree.get_tree_element(path)

            chunks = split(input.read(), len(config['nodes']))
            indexes = block_manager.put_blocks(chunks)            
            dfs_tree.put(folder, name, indexes)

    if args.get_path:
        tree_element = dfs_tree.get_tree_element(args.get_path)
        indexes = dfs_tree.get_file_indexes(tree_element)
        print '\n'.join(block_manager.read_blocks(indexes))
        
    block_manager.save()
    dfs_tree.save()


if __name__ == "__main__":
    main()


