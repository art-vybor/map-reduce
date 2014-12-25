with open('out', 'r') as f:
	d = {}
	for line in f:
		l = line.split()

		for word in l:
			if word in d:
				d[word]+=1
			else:
				d[word]=1
			
print d
		