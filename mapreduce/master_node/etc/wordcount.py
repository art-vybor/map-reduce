def map(input):
    for row in input:   
        for word in row.split():
            yield (word,1)

def reduce(key, values):
    return (key, sum(values))