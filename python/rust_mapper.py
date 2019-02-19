#!/usr/bin/env python3

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
	line = line.strip()
	id, all_data = line.split("\t")
	data = all_data.split(",")
	print('%s\t%s' % (data[4].split(":")[1][:4], data[0].split(":")[1]))