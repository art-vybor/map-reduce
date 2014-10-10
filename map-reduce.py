# split -> map -> combine -> sort -> reduce
num_of_map = 4
num_of_reduce = 3


#n > 0
def split(input, n):
    rows = input.split('\n') #TODO rewrite for big file
    chunk_size = int(len(rows)/n) #TODO check python version and remove int()
    if chunk_size != 0:
        for i in xrange(0, n-1, chunk_size):
            yield rows[i:i+chunk_size]
        yield rows[chunk_size*(n-1):len(rows)] #TODO make more uniformly
    else:
        for i in xrange(0, n, 1):
            yield rows[i:i+1] #slice should not be replaced by index

def map(mapFunc, chunks):
    for chunk in chunks:
        yield ([x for x in map_func(chunk)])    
    
def combine(reduceFunc, chunks):
    for chunk in chunks:
        chunk = sorted(chunk)       

        new_chunk = []
        new_pair = (chunk[0][0], [])

        for pair in chunk:          #TODO put in function
            if pair[0] == new_pair[0]:
                new_pair[1].append(pair[1])
            else:
                new_chunk.append(reduceFunc(*new_pair))
                new_pair = (pair[0], [pair[1]])
        new_chunk.append(reduceFunc(*new_pair))

        yield new_chunk

def partition(key, num_of_reduce):
    return (key.__hash__()) % num_of_reduce

def sort(map_chunks, num_of_reduce):
    def pair_from_chunk(i):
        return map_chunks[i][index[i]]

    map_chunks = [x for x in map_chunks]
    num_of_map = len(map_chunks)
    reduce_pairs = []
    reduce_chunks = []
    for i in range(0, num_of_reduce):
        reduce_chunks.append([])
    
    index = {i:0 for i in range(0, num_of_map) if len(map_chunks[i]) != 0}

    while len(index) > 0:
        smallest = min(index, key=lambda x: pair_from_chunk(x))
        reduce_pairs.append(pair_from_chunk(smallest))
#
        index[smallest] += 1
        if index[smallest] == len(map_chunks[smallest]):
            del index[smallest]


    new_pair = (reduce_pairs[0][0], [])
    for pair in reduce_pairs:
        if pair[0] == new_pair[0]:
            new_pair[1].append(pair[1])
        else:
            reduce_chunks[partition(new_pair[0], num_of_reduce)].append(new_pair)
            new_pair = (pair[0], [pair[1]])

    reduce_chunks[partition(new_pair[0], num_of_reduce)].append(new_pair)

    for chunk in reduce_chunks:
        yield chunk

def reduce(chunks, reduce_func):
    for chunk in chunks:
        yield [reduce_func(*x) for x in chunk]


input='foo bar bar\nkey key true\nbar bar bar\nfoo foo bar\nfoo bar foo'
def map_func(input):
    for row in input:   
        for word in row.split():
            yield (word,1)

def reduce_func(key, values):
    return (key, sum(values))
            
            
chunks = split(input, num_of_map)
chunks = map(map_func, chunks)
chunks = combine(reduce_func, chunks)
chunks = sort(chunks, num_of_reduce)
chunks = reduce(chunks, reduce_func)

import itertools

chunks = list(itertools.chain(*chunks))
for x in chunks:
    print x
    
    
 
 
        
        
        