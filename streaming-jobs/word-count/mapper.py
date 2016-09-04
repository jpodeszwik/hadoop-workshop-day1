#!/usr/bin/env python2

import fileinput

for line in fileinput.input():
	for word in line.split():
		print '{}\t{}'.format(word, 1)
	
