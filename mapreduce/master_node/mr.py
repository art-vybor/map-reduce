import json
import argparse
import os
import pickle

from dfs.dfs_tree import dfs_tree
from dfs.block_manager import block_manager
from nodes_manager import nodes_manager


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

    while not nm.all_thread_completed():
        a = 1

    result = nm.flush_q()

    for x in result:
        print x

    #wait until all map completed
    #sort stage
    #reduce stage

    dt.save()
    bm.save()

if __name__ == "__main__":
    main()