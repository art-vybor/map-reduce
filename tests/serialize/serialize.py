import string
import random
from time import time

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

d = {}
for i in range(0, 100):
    l = []
    for j in range(0, 10000):
        l.append(random.randint(0,1000000))
    d[id_generator()] = l


#d = ''
#for i in range(0,100000):
#    d += id_generator(100)

# print 'len: %s' % len(d)

def make_test(encode, decode, protocol=None):

    start = time()
    if protocol is not None:
        d_encode = encode(d, protocol=protocol)
    else:
        d_encode = encode(d)
    encode_time = time() - start
    length = len(d_encode)

    start = time()
    d_decode = decode(d_encode)
    decode_time = time() - start

    if protocol is not None:
        print '---%s(%s)----' % (encode.__module__, protocol)
    else:
        print '---%s----' % (encode.__module__)
    print 'encode time: %s' % (encode_time)
    print 'decode time: %s' % (decode_time)
    print 'total time: %s' % (decode_time+encode_time)
    print 'length %s' % length


import pickle
import cPickle
import json
import cjson
import marshal

make_test(pickle.dumps, pickle.loads)
make_test(pickle.dumps, pickle.loads, protocol=0)
make_test(pickle.dumps, pickle.loads, protocol=1)
make_test(pickle.dumps, pickle.loads, protocol=2)
make_test(cPickle.dumps, cPickle.loads)
make_test(cPickle.dumps, cPickle.loads, protocol=0)
make_test(cPickle.dumps, cPickle.loads, protocol=1)
make_test(cPickle.dumps, cPickle.loads, protocol=2)
make_test(json.dumps, json.loads)
make_test(cjson.encode, cjson.decode)
make_test(marshal.dumps, marshal.loads)