def map_func(input):
    domen = input.split()
    if int(domen[1]) > 500:
        yield (domen[1], input)

def reduce_func(key, values):
    return (key, values)
