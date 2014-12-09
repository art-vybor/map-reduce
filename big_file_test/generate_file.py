a = 'ABCDE FDRED sadasdjk 213123123 qwert cvbnm 0980909\n'

mb = 20000

with open('out_mini', 'w') as file:
    for i in range(1, 100*mb):
        file.write(a)

#20000 - 1mb