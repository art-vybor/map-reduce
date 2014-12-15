import zmq
import marshal
import os
import threading
from Queue import Queue
from copy import deepcopy


class nodes_manager:
    def __init__(self, config, port='mr_port'):
        self.nodes = deepcopy(config['nodes'])
        self.working_threades = 0
        self.q = Queue()

        for node in self.nodes:
            node['thread'] = 0
            node['socket'] = zmq.Context().socket(zmq.REQ)
            node['socket'].connect('{URL}:{PORT}'.format(URL=node['url'], PORT=node[port]))

    def wait_recv(self, node):
        result = node['socket'].recv()
        node['thread'] = 0
        self.q.put(marshal.loads(result))

    def send(self, node_index, func_word, data=None):
        node = self.nodes[node_index]
        request = {func_word: data, 'dfs_port': node['dfs_port'], 'node': node_index}
        node['socket'].send(marshal.dumps(request))
        if node['thread'] != 0:
             raise Exception("You can't allowed to create second thread for node: {NODE}".
                    format(NODE=node['url']))
        node['thread'] = 1
        threading.Thread(target = self.wait_recv, args = (node, )).start()

    def all_threads_completed(self):
        for node in self.nodes:
            if node['thread'] == 1:
                return False
        return True

    def flush_q(self):               
        self.wait()

        result = []
        while not self.q.empty():
            result.append(self.q.get())

        return result

    def wait(self):
        while not self.all_threads_completed():
            pass