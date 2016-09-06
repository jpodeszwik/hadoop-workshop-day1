#!/usr/bin/env python

import fileinput

for line in fileinput.input():
	count, word = line.split()
	print '{}\t{}'.format(word, count)

