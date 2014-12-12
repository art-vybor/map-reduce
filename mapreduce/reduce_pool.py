from lib.nodes_manager import nodes_manager

class reduce_pool:
    def __init__(self, config):
        self.nm = nodes_manager(config)

        self.pool = {}
        for node in range(0, len(self.nm.nodes)):
            self.pool[node] = {}

    def get_node(self, key):
        num_of_nodes = len(self.pool)
        return (key.__hash__()) % num_of_nodes

    def send(self, node):
        self.nm.send(node, 'reduce_pairs', self.pool[node])
        self.pool[node] = {}

    def add(self, kv_dict):
        for key, value in kv_dict.iteritems():
            node = self.get_node(key)
            d = self.pool[node]

            if key in d:
                d[key].extend(value)
            else:
                d[key] = value

        self.flush()

    def flush(self, force=False, key_limit=100):
        #TODO replace key_limit with size_limit
        self.nm.wait()

        for node in self.pool:
            if force:
                self.send(node)
            else:
                if len(self.pool[node]) > key_limit:
                    self.send(node)


        
