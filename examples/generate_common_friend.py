from copy import deepcopy
import random
import string

user_count = 400
max_friends = 300
min_friends = 10

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

#generate users
users = []

for i in range(0, user_count):
    users.append(id_generator())

#generate friends
with open('in_common_friends_%s_%s' % (user_count, max_friends), 'w') as output:
    for user in users:
        tmp_users = deepcopy(users)

        friends = []
        for i in range(0, random.randint(min_friends, max_friends)):
            friend_index = random.randint(0, len(tmp_users)-1)
            friends.append(tmp_users[friend_index])
            tmp_users.pop(friend_index)

        output.write('%s - %s\n' % (user, ' '.join(friends)))