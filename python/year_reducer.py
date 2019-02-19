#!/usr/bin/env python3

from operator import itemgetter
import sys
from collections import Counter

current_year = None
current_tag = Counter()
year = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    year, tag = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: year) before it is passed to the reducer
    if current_year == year:
        current_tag.update([tag])
    else:
        if current_year:
            # write result to STDOUT
            print('%s\t%s' % (current_year, current_tag.most_common(1)))
        current_tag = Counter([tag])
        current_year = year

# output the last year if needed!
# if current_year == year:
#     print('%s\t%s' % (current_year, current_tag.most_common(1)))