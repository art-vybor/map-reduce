def map_func(input):
    [person, friends] = input.split(' - ')
    friends = friends.split()

    for friend in friends:
        result = sorted([person, friend])
        yield ('%s %s:' % tuple(result), friends)

def reduce_func(key, values):
    print key, values
    result = ''
    if len(values) == 2:
        result = ' '.join(set(values[0]).intersection(set(values[1])))


    return (key, result)