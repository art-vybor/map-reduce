def map_func(string):
    [person, friends] = string.split(' - ')
    friends = friends.split()

    for friend in friends:
        yield ('%s %s: ' % tuple(sorted([person, friend])), friends)

def reduce_func(key, values):
    result = ''
    if len(values) == 2:
        result = ' '.join(set(values[0]).intersection(set(values[1])))
    return [(key, result)]