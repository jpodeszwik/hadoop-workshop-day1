#!/usr/bin/env python

import fileinput

for line in fileinput.input():
	try:
		word, count = line.split()
		print '{}\t{}'.format(count, word)
	except:
		pass
