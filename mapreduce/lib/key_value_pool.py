from lib.nodes_manager import nodes_manager
#from pympler.asizeof import asizeof
from time import time

class key_value_pool:
    def __init__(self, config):
        self.nm = nodes_manager(config, 'mr_slave_port')

        self.pool = {}
        for node in range(0, len(self.nm.nodes)):
            self.pool[node] = {}

    def get_node(self, key):
        num_of_nodes = len(self.pool)
        node = (key.__hash__()) % num_of_nodes
        return node

    def flush_node(self, node):
        self.nm.send(node, 'add_pairs', self.pool[node])
        self.pool[node] = {}

    # def add(self, key, value):        
    #     d = self.pool[self.get_node(key)] #TODO compare speed with add_dict approach
    #     if key in d:
    #         d[key].append(value)
    #     else:
    #         d[key] = [value]


    def add_dict(self, kv_dict):
        for key, value in kv_dict.iteritems():
            d = self.pool[self.get_node(key)]

            if key in d:
                d[key].extend(value)
            else:
                d[key] = value

        self.flush()

    def flush(self, force=False, size_limit_mb=64):
        #size_limit = size_limit_mb * 1024 * 1024

        for node in self.pool:
            #if force:
            self.flush_node(node)
            #else: #TODO add size_limit
            #    self.flush_node(node)


        self.nm.wait()