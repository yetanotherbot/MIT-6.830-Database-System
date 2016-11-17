import random

with open('data.txt', 'w') as f:
    for i in range(10000):
        a = random.randint(0, 10)
        b = random.randint(0, 1000)
        f.write(str(a) + "," + str(b) + "\n")

