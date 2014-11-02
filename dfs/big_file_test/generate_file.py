a = 'ABCDE FDRED sadasdjk 213123123 qwert cvbnm 0980909\n'

with open('out_mini', 'w') as file:
    for i in range(1, 20000*1000):
        file.write(a)

#20000 - 1mb