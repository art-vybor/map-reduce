def map_func(input):
    #result = []
    for word in input.split():
        yield (word, 1)
    #return result

#from array import array

blocks = []
for i in [382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397]:
    print i    
    with open('/home/avybornov/dfs_storage/%i' % i, 'r') as block_file:
        block = block_file.read()
        res = {}
        for x in map_func(block):
            if x[0] in res:
                res[x[0]].append(x[1])
            else:
                res[x[0]] = [x[1]]
        blocks.append(sorted(res))