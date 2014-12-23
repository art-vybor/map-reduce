def map_func(input):
    for word in input.split():
        yield (word, 1)

def reduce_func(key, values):
    return (key, sum(values))