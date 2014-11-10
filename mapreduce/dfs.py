import argparse
import json
import os

from lib.dfs_tree import dfs_tree
from lib.block_manager import block_manager
from lib.dfs_lib import split

def parse_config(config_path):
    with open(config_path) as config_file:
        config_json = json.load(config_file)

        return config_json

config_path = os.path.join(os.path.dirname(__file__), 'etc/config.json')
config = parse_config(config_path)

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

def main():
    args = parse_args()

    dt = dfs_tree()
    bm = block_manager(config)

    if args.ls_path:
        tree_element = dt.get_tree_element(args.ls_path)
        print '\n'.join(dt.ls(tree_element))

    if args.mkdir_path:
        dt.mkdir(args.mkdir_path)

    if args.rm_path:
        tree_element = dt.get_tree_element(args.rm_path)
        indexes = dt.get_recursive_files_indexes(tree_element)
        bm.remove_blocks(indexes)
        dt.rm(tree_element)

    if args.put_paths:
        path = args.put_paths[1]
        if path.endswith('/'):
            raise Exception("Can't write file without name.")
        path, name = path.rsplit('/', 1)    
        folder = dt.get_tree_element(path)
        

        with file(args.put_paths[0]) as input_file:
            blocks = split(input_file, lambda x: x)
            indexes = bm.write_blocks(blocks)
            dt.put(folder, name, indexes)

    if args.get_path:
        tree_element = dt.get_tree_element(args.get_path)
        indexes = dt.get_file_indexes(tree_element)
        for block in bm.read_blocks(indexes):
            print (block), #braces with comma for removing newline
        
    bm.save()
    dt.save()

if __name__ == "__main__":
    main()