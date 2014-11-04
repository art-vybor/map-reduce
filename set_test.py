from threading import Thread
import threading

from time import sleep

d = 5

def threaded_function(arg):
    global d
    d -= arg
    #print d
    sleep(arg)
    print "running %d" % arg
        
        


if __name__ == "__main__":
    thread1 = Thread(target = threaded_function, args = (1, ))
    thread2 = Thread(target = threaded_function, args = (2, ))
    print threading.active_count()
    thread1.start()
    print threading.active_count()
    thread2.start()
    print threading.active_count()
    #thread.join()
    print "thread finished...exiting"