import zmq
import pickle
import os
import threading
from Queue import Queue


class nodes_manager:
    def __init__(self, config):
        self.nodes = config['nodes']
        self.working_threades = 0
        self.q = Queue()

        for node in self.nodes:
            node['socket'] = zmq.Context().socket(zmq.REQ)
            node['socket'].connect('{URL}:{PORT}'.format(URL=node['url'], PORT=node['mr_port']))

    def wait_result(self, node):
        result = node['socket'].recv()
        self.q.put(pickle.loads(result))

    def send(self, node_index, func_word, data):
        node = self.nodes[node_index]
        request = {func_word: data}
        node['socket'].send(pickle.dumps(request))
        threading.Thread(target = self.wait_result, args = (node, )).start()

    def all_thread_completed(self):
        return threading.active_count() == 1

    def flush_q(self):
        result = []
        if self.all_thread_completed():
            while not self.q.empty():
                result.append(self.q.get())
        return result

