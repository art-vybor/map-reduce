import string
import random
from time import time

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

d = {}
for i in range(0, 100000):
    d[id_generator()] = [random.randint(0,1000000), random.randint(0,100000), random.randint(0,10000000), random.randint(0,10000)]

def make_test(func):
    start = time()
    d_encode = func(d)
    total_time = time() - start
    length = len(d_encode)

    print '---%s-----%s------' % (func.__module__, func.__name__)
    print 'time: %s' % total_time
    print 'length %s' % length


import pickle
import cPickle
import json
import cjson

make_test(pickle.dumps)
make_test(cPickle.dumps)
make_test(json.dumps)
make_test(cjson.encode)
