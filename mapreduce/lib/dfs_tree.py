import ast
from lxml import etree
import os

class dfs_tree:
    def __init__(self):
        self.fs_file = os.path.join(os.path.dirname(__file__), '../etc/fs_structure.xml')

        parser = etree.XMLParser(remove_blank_text=True)
        self.tree = etree.parse(self.fs_file, parser)

    def save(self):
        self.tree.write(self.fs_file, pretty_print=True)

    def is_exist(self, path):
        tree_element = self.tree.xpath('/root' + path.rstrip('/'))

        return len(tree_element) == 1

    def get_tree_element(self, path):
        tree_element = self.tree.xpath('/root' + path.rstrip('/'))

        if len(tree_element) == 1:
            return tree_element[0]
        else:
            raise Exception('Path {PATH} is incorrect'.format(PATH=path))

    def is_folder(self, tree_element):
        return tree_element.get('index_list') is None

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
            raise Exception('File already exists')

        folder.append(etree.Element(name, index_list=str(index_list)))

    def ls(self, tree_element):
        ls = []
        for element in self.get_files_from_tree_element(tree_element):
            ls.append(element.tag + '/' if self.is_folder(element) else element.tag)

        return ls

    def rm(self, tree_element):
        tree_element.getparent().remove(tree_element)
