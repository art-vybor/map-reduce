import json
import argparse
import os

from time import time
from lib.dfs_tree import dfs_tree
from lib.block_manager import block_manager
from lib.nodes_manager import nodes_manager


def parse_config(config_path):
     with open(config_path) as config_file:
         config_json = json.load(config_file)
         return config_json

config_path = os.path.join(os.path.dirname(__file__), 'etc/config.json')
config = parse_config(config_path)

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce command line interface.')

    parser.add_argument('-i', help='path to input data placed in dfs',
        metavar='dfs_path', dest='input_path', required=True)
    parser.add_argument('-o', help='path to output data in dfs',
        metavar='dfs_path', dest='output_path', required=True)
    parser.add_argument('-mr', help='python file with map and reduce functions',
        metavar='path', dest='mr_path', required=True)
    
    return parser.parse_args()

def merge(pairs_dicts):
    result = {}

    for pairs_dict in pairs_dicts:

        for key in pairs_dict:
            if key in result:
                result[key].extend(pairs_dict[key])
            else:
                result[key] = pairs_dict[key]

    return result

def get_rand_node(key, num_of_nodes):
    return (key.__hash__()) % num_of_nodes

def get_reduce_blocks_by_node(pairs_dict, num_of_nodes):
    result = {}

    for key in pairs_dict:
        node = get_rand_node(key, num_of_nodes)
        if node in result:
            result[node].append((key, pairs_dict[key]))
        else:
            result[node] = [(key, pairs_dict[key])]

    return result

def do_map(nm, indexes_by_node, mr_file):
    for node in indexes_by_node:
        data = (indexes_by_node[node], mr_file)
        nm.send(node, 'map', data)    

    while not nm.all_thread_completed():
        pass

    return nm.flush_q()

def do_reduce(nm, blocks_by_node):
    for node in blocks_by_node:
        data = blocks_by_node[node]
        nm.send(node, 'reduce', data)

    while not nm.all_thread_completed():
        pass

    return nm.flush_q()
def rename_reduce_results(bm, reduce_results):
    indexes = []

    for reduce_result in reduce_results:
        indexes_for_node, node_index = reduce_result

        for _index in indexes_for_node:
            index = bm.get_index()
            
            if bm.change_block_index(_index, index, node_index):
                indexes.append(index)   
                bm.index_map[index] = node_index
            else:
                raise Exception("Can't change index {OLD} to {NEW} by node {NODE}".format(OLD=_index, NEW=index, NODE=node_index))
    return indexes


def main():
    args = parse_args()

    dt = dfs_tree()
    bm = block_manager(config)
    nm = nodes_manager(config)

    input_file = dt.get_tree_element(args.input_path)
    output_dirname = dt.get_tree_element(os.path.dirname(args.output_path))

    if dt.is_folder(input_file): raise Exception('Input data: {PATH} must be a file'.format(PATH=args.input_path))
    if not dt.is_folder(output_dirname): raise Exception('Output data: {PATH} must be placed in folder'.format(PATH=args.output_path))
    if dt.is_exist(args.output_path): raise Exception('Output data {PATH} is exist'.format(PATH=args.output_path))

    mr_file = ""
    with open(args.mr_path, 'r') as f:
        mr_file = f.read()

    print 'task %s started' % (args.mr_path)
    start = time()

    #map
    indexes = dt.get_file_indexes(input_file)
    indexes_by_node = bm.split_indexes_by_node(indexes)

    map_result = do_map(nm, indexes_by_node, mr_file)
    
    print 'map finished %ss' % (time() - start)
    start = time()

    #merge
  
    merged_result = merge(map_result)    
    blocks_by_node = get_reduce_blocks_by_node(merged_result, len(nm.nodes))

    print 'merge finished %ss' % (time() - start)
    start = time()

    #reduce
    reduce_results = do_reduce(nm, blocks_by_node)

    indexes = rename_reduce_results(bm, reduce_results)
    
    _, name = args.output_path.rsplit('/', 1)    
    dt.put(output_dirname, name, indexes)

    print 'reduce finished %ss' % (time() - start)

    dt.save()
    bm.save()

if __name__ == "__main__":
    main()