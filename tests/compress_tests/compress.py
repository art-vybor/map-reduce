import zlib
import bz2
from time import time

string = open('../../examples/groupby/groupby_100000_40', 'r').read()
str_zip = ''

print 'source: %.2fkk' % (len(string)*1.0/1000/1000)

for i in range(0,7):
    start = time()
    str_zip = zlib.compress(string, i)
    print 'compress %d: %.2fkk %.2fs' % (i, len(str_zip)*1.0/1000/1000, time()-start)

start = time()
str = zlib.decompress(str_zip)
print 'decompress: %.2fs' % (time()-start)

print '----------bz2-----------'
for i in range(1,6):
    start = time()
    str_zip = bz2.compress(string, i)
    print 'compress %d: %.2fkk %.2fs' % (i, len(str_zip)*1.0/1000/1000, time()-start)

start = time()
str_zip = bz2.decompress(str_zip)
print 'decompress: %.2fs' % (time()-start)

