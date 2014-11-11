import json
import argparse
import os
import pickle

from time import sleep
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

def merge(pairs_list):
    def append_to_result(res, el):
        if len(res) > 0 and res[len(res)-1][0] == el[0]:
            res[len(res)-1] = (res[len(res)-1][0], res[len(res)-1][1] + el[1])
        else:
            res.append(el)     

    result = []

    while len(pairs_list) > 0:
        min_idx = 0

        for i in range(1, len(pairs_list)):
            if pairs_list[i][0] < pairs_list[min_idx][0]:
                min_idx = i

        append_to_result(result, pairs_list[min_idx][0])
        
        pairs_list[min_idx] = pairs_list[min_idx][1:len(pairs_list[min_idx])]
        if len(pairs_list[min_idx]) == 0:
            pairs_list.pop(min_idx)
        
    return result


def merge1(pairs_dicts):
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

def get_reduce_blocks_by_node(pairs, num_of_nodes):
    result = {}

    for pair in pairs:
        node = get_rand_node(pair[0], num_of_nodes)
        result[node] = result[node] + [pair] if node in result else [pair]

    return result

def get_reduce_blocks_by_node1(pairs_dict, num_of_nodes):
    result = {}

    for key in pairs_dict:
        node = get_rand_node(key, num_of_nodes)
        if node in result:
            result[node].append((key, pairs_dict[key]))
        else:
            result[node] = [(key, pairs_dict[key])]

    return result


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


    #map
    indexes = dt.get_file_indexes(input_file)
    indexes_by_node = bm.split_indexes_by_node(indexes)

    

    for node in indexes_by_node:
        data = (indexes_by_node[node], mr_file)
        nm.send(node, 'map', data)

    print 'map started'

    while not nm.all_thread_completed():
        pass

    print '  -  finished'
    map_result = nm.flush_q()

    print 'merge started'
    #merge
    merged_result = merge1(map_result)
    
    blocks_by_node = get_reduce_blocks_by_node1(merged_result, len(nm.nodes))

    print '  -  finished'

    #reduce
    print 'reduce started'
    for node in blocks_by_node:
        data = blocks_by_node[node]
        nm.send(node, 'reduce', data)

    while not nm.all_thread_completed():
        pass

    reduce_results = nm.flush_q()

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
    print '  -  finished'            

    print indexes
    _, name = args.output_path.rsplit('/', 1)    
    dt.put(output_dirname, name, indexes)
    

    
    #print reduce_result

    dt.save()
    bm.save()

if __name__ == "__main__":
    main()