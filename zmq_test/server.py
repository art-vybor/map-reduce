import zmq
import time
import sys

socket = zmq.Context().socket(zmq.REP)
socket.bind("tcp://*:5556")

while True:
    message = socket.recv()
    print "Received request: ", message
    socket.send("world")