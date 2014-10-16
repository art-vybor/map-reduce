import argparse
from lxml import etree
import pickle

class DfsTree:
    def __init__(self, fs_file):
        self.fs_file = fs_file

        parser = etree.XMLParser(remove_blank_text=True)
        self.tree = etree.parse(self.fs_file, parser)

    def save(self):
        self.tree.write(self.fs_file, pretty_print=True)

    def mkdir(self, path):
        path_tokens = filter(lambda x: len(x) > 0, path.split('/'))
        folder = self.tree.getroot()

        for token in path_tokens:
            next_folder = folder.find(token)

            if next_folder is None:
                folder.append(etree.Element(token, folder="True"))
            elif not self.is_folder(next_folder):
                raise Exception('Path {PATH} does not be created: {TOKEN} is file.'.format(PATH=path, TOKEN=token))

            folder = folder.find(token)

    def is_folder(self, folder):
        return folder.get('folder') == 'True'

    def get_folder(self, path):
        folder = self.tree.xpath('/root' + path.rstrip('/'))

        return folder[0] if len(folder) == 1 else None

    def put(self, path, name, index_list):
        folder = self.get_folder(path)

        if folder is None:
            raise Exception('Path {PATH} is incorrect'.format(PATH=path))

        if folder.find(name) is not None:
            raise Exception('File {PATH}/{NAME} already exists'.format(PATH=path.rstrip('/'), NAME=name))
        folder.append(etree.Element(name, index_list=str(index_list)))

    def ls(self, path):
        folder = self.get_folder(path)

        if folder is None:
            raise Exception('Path {PATH} is incorrect'.format(PATH=path))

        ls = []

        if self.is_folder(folder):
            for element in folder:
                ls.append(element.tag + '/' if self.is_folder(element) else element.tag)
        else:
            ls.append(folder.tag)            

        return ls

    def rm(self, path):
        path_tokens = filter(lambda x: len(x) > 0, path.split('/'))
        folder = self.tree.getroot()

        for token in path_tokens:
            next_folder = folder.find(token)

            if next_folder is None:
                raise Exception('Path {PATH} is incorrect'.format(PATH=path))

            folder = next_folder

        folder.getparent().remove(folder)


class BlockManager:
    def __init__(self, bm_file, num_node):
        self.bm_file = bm_file
        self.num_node  = num_node
        self.index_map = pickle.load(open(self.bm_file, 'rb'))

    def save(self):
        pickle.dump(self.index_map, open(self.bm_file, 'wb'))

 #   def put_blocks():



#n > 0
# def split(input, n):
#     rows = input.split('\n') #TODO rewrite for big file
#     chunk_size = int(len(rows)/n) #TODO check python version and remove int()
#     if chunk_size != 0:
#         for i in xrange(0, n-1, chunk_size):
#             yield rows[i:i+chunk_size]
#         yield rows[chunk_size*(n-1):len(rows)] #TODO make more uniformly
#     else:
#         for i in xrange(0, n, 1):   
#             yield rows[i:i+1] #slice should not be replaced by index

#def main():
num_nodes = 3;

parser = argparse.ArgumentParser(description='Dfs command line interface.')

parser.add_argument('-ls', nargs=1, help='list of directory',
    metavar='path', dest='ls_path')
parser.add_argument('-mkdir', nargs=1, help='make new directory',
    metavar='path', dest='mkdir_path')
parser.add_argument('-rm', nargs=1, help='remove file or folder',
    metavar='path', dest='rm_path')
parser.add_argument('-put', nargs=2, help='put local file to dfs',
    metavar=('local_path', 'dfs_path'), dest='put_paths')

args = parser.parse_args()

dfs_tree = DfsTree('fs_structure.xml')
#block_manager = BlockManager('bm.pickle', num_nodes)
#dfs_tree.put('/', 'test_file', [1,2,3])

if args.ls_path:
    print '\n'.join(dfs_tree.ls(args.ls_path[0]))

if args.mkdir_path:
    dfs_tree.mkdir(args.mkdir_path[0])

if args.rm_path:
    dfs_tree.rm(args.rm_path[0])    

if args.put_paths:
    with open(args.put_paths[0], 'r') as input:
        chunks = split(input_file.read(), 3)





dfs_tree.save()


#if __name__ == "__main__":
#    main()


