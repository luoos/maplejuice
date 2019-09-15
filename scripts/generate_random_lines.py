import string
import random
word_file = "/usr/share/dict/words"
WORDS = open(word_file).read().splitlines()
appear_in_one_file = random.randint(1,10)
for n in xrange(1, 11):
	with open("scripts/logs/random"+str(n)+".log", 'w') as f:
		for _ in xrange(1000):
			r = ' '.join([random.choice(WORDS) for _ in xrange(6)])
			if random.randint(1,10) == 1:
				f.write("Imfreqent\n")
			if random.randint(1,50) == 1:
				f.write("ImsomewhatFrequent")
			if random.randint(1,5000) == 1:
				f.write("Imrare")
			f.write(r+'\n')
		if appear_in_one_file == n:
			f.write("ImtheOnlyOne")
		if n % 2 == 1:
			f.write("IminSomeFiles")
		f.write("IminAllFiles")
