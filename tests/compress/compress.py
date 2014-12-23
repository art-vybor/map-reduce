import zlib
import bz2
from time import time

string = open('../../mapreduce/groupby_1000000_400', 'r').read()[0:100*1000*1000]

source_size = len(string)*1.0/1024/1024
print 'source: %.2fkk' % (source_size)

for i in range(1,10):
    start = time()
    str_zip = zlib.compress(string, i)
    compress_time = time()-start

    start = time()
    zlib.decompress(str_zip)
    decompress_time = time()-start

    compress_size = len(str_zip)*1.0/1024/1024

    coef = (source_size-compress_size)/(compress_time+decompress_time)*8

    print 'zlib %d: %.2f' % (i, coef)

for i in range(1,10):
    start = time()
    str_zip = bz2.compress(string, i)
    compress_time = time()-start
    
    start = time()
    bz2.decompress(str_zip)
    decompress_time = time()-start

    compress_size = len(str_zip)*1.0/1024/1024

    coef = (source_size-compress_size)/(compress_time+decompress_time)*8

    print 'bz2 %d: %.2f' % (i, coef)



