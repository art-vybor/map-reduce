from copy import deepcopy
import random
import string

user_count = 100000
max_friends = 300
min_friends = 10

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

#generate users
users = set()

while len(users) < user_count:
    users.add(id_generator())

users = list(users)

#generate friends
with open('in_common_friends_%s_%s' % (user_count, max_friends), 'w') as output:
    for user in users:

        friends = set()
        len_friends = random.randint(min_friends, max_friends)

        while len(friends) < len_friends:
            friends.add(users[random.randint(0, len(users)-1)])

        output.write('%s - %s\n' % (user, ' '.join(friends)))