from array import array
from time import time
import numpy as np

length = 1000*1000*100

l_gen = [x for x in range(length, 0, -1)]

l_list = []
for x in range(length, 0, -1): l_list.append(x)

l_array = array('i')
for x in range(length, 0, -1): l_array.append(x)

np_array = np.array(l_array)
#for x in range(length, 0, -1): np_array.append(x)

print 'generated'

start = time()
l_gen.sort()
print 'l_gen time %s' % (time()-start)


start = time()
l_list.sort()
print 'l_list time %s' % (time()-start)

start = time()
c = sorted(l_array)
print 'l_array time %s' % (time()-start)

start = time()
d = np.sort(np_array)
print 'np_array time %s' % (time()-start)

#print l_gen, l_list,c,d