block_index = 0
block_len_limit = 64*1024*1024
block_len = 0
block = []

import collections

def dump():
    global block_index
    global block_len
    global block

    with open('res_%d' % block_index, 'w') as o:
        block = sorted(block)
        for key, line in block:
            o.write('%s\t%s' % (key, line))
        block_index += 1
        block_len = 0
        block = []

with open('../../mapreduce/groupby_1000000_400', 'r') as i:
    for line in i:        
        domen = line.split()

        if int(domen[1]) > 500:
            block.append((domen[1], line))
            block_len += len(line) + len(domen[1]) + 1

        if block_len > block_len_limit:
            dump()

    if block_len != 0:
        dump()

def head_next(f, f_head, i):
    line = f[i].readline()
    if not line:
        f.pop(i)
        f_head.pop(i)
    else:
        f_head[i] = line.split('\t', 1)

f = []
f_head = []
for i in range(0, block_index):
    f.append(open('res_%d' % i, 'r'))
    f_head.append([])


for i in range(0, len(f)):
    head_next(f, f_head, i)

print f

out = open('out', 'w')
while len(f)>0:
    f_min_idx = 0

    for i in range(0, len(f)):
        if f_head[i][0] < f_head[f_min_idx][0]:
            f_min_idx = i

    f_min = f_head[f_min_idx][0]
    i = 0
    while i < len(f):
        if f_head[i][0] == f_min:
            out.write(f_head[i][1])
            head_next(f, f_head, i)
        else:
            i+=1

