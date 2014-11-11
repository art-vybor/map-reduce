def map_func(input):
    result = []
    for word in input.split():
        result.append((word, 1))
    return result

def reduce_func(key, values):
    return (key, sum(values))