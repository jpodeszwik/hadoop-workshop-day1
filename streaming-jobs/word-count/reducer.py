#!/usr/bin/env python2

import fileinput

current_key = None
current_sum = 0

for line in fileinput.input():
	key, count = line.split()
	
	if key == current_key:
		current_sum += int(count)
	else:
		if current_key != None:
			print '{}\t{}'.format(current_key, current_sum)	
		current_key = key
		current_sum = int(count)

if current_key != None:
	print '{}\t{}'.format(current_key, current_sum)
