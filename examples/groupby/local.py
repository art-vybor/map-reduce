kv_pool = {}
kv_pool_index = 0
block_len_limit = 1024*1024
block_len = 0
import collections

def dump():
    global kv_pool
    global kv_pool_index
    global block_len

    with open('res_%d' % kv_pool_index, 'w') as o:
        kv_pool = collections.OrderedDict(sorted(kv_pool.items()))
        for key, value in kv_pool.iteritems():
            o.write('%s %s\n' % (key, ' '.join(value)))
        kv_pool = {}
        kv_pool_index += 1
        block_len = 0

# with open('../../mapreduce/groupby_1000000_400', 'r') as i:
#     for line in i:
#         domen = line.split()
#         if int(domen[1]) > 50:
#             block_len+=len(domen[0]) + 2

#             if domen[1] in kv_pool:
#                 kv_pool[domen[1]].append(domen[0])
#             else:
#                 block_len+=len(domen[1])
#                 kv_pool[domen[1]] = [domen[0]]

#         if block_len > block_len_limit:
#             dump()

#     if block_len != 0:
#         dump()
kv_pool_index = 2
f = []
f_head = {}
for i in range(0, kv_pool_index):
    f.append(open('res_%d' % i, 'r'))

for i in range(0, len(f)):
    _f = f[i]
    line = _f.readline()
    if not line: f.pop(i)

    f_head[i] = tuple(line.split(' ', 1))

while len(f)>0:
    f_min = 0

    #for i in range(0, len(f)):
        #if f_head[i][0] < f_head[f_min][0]:
            #f_head[i][0] = 

print f_head

    #f_head = 
    #f.next()

