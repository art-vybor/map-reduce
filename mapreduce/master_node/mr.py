#import json
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Map-reduce command line interface.')

    parser.add_argument('-i', help='path to input data placed in dfs',
        metavar='dfs_path', dest='input_path', required=True)
    parser.add_argument('-o', help='path to output data in dfs',
        metavar='dfs_path', dest='output_path', required=True)
    parser.add_argument('-mr', help='python file with map and reduce functions',
        metavar='path', dest='mr_path', required=True)
    
    return parser.parse_args()

# def parse_config(config_path='etc/config.json'):
#     with open(config_path) as config_file:
#         config_json = json.load(config_file)
#         return config_json

def main():
    args = parse_args()

    input_path = args.input_path
    ouput_path = args.output_path
    mr = args.mr_path

    #if input path is a file and output path is not exist and output path dirname is exist do
        #get indexes of input file
        #for each index:
            #send ('map', mr_file) 

        #wait until all map completed
        #sort stage
        #reduce stage

    # config = parse_config()

if __name__ == "__main__":
    main()