import zmq
import sys
from time import time
socket = zmq.Context().socket(zmq.REQ)
socket.connect ("tcp://localhost:5546")
print "Sending request..."

start = time()
socket.send ("hello")
message = socket.recv()
print 'time: %d' % (time() - start)
print "Received reply: %s." % message