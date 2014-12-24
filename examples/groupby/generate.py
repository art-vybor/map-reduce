import random

n = 10 #num rows
m = 4 #num ceils

k = 1000 #field range

with open ('groupby_%d_%d' % (n, m), 'w') as f:
    for i in range(1, n+1):
        domen = [i]
        for j in range(1, m):
            domen.append(random.randint(0,k))

        f.write('\t'.join(map(str, domen)) + '\n')
