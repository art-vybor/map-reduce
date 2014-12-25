def map_func(input):
    domen = input.split()
    if int(domen[1]) > 850:
        yield (domen[1], input)

def reduce_func(key, values):
    for value in values:
        yield (key, value)
