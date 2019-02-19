#!/usr/bin/env python3

from operator import itemgetter
import sys
from collections import Counter
import re

current_year = None
count = Counter()
year = None
regex = lambda x: re.search("(^rust$)|(^rust-.*$)|(^rustup$)|(^rustdoc$)", x)

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    year, tag = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: year) before it is passed to the reducer
    if current_year == year:
        if regex(tag):
            count.update([year])
    else:
        if current_year:
            # write result to STDOUT
            curr = 0 if len(count.most_common(1)) == 0 else count.most_common(1)[0][1]
            print('%s\t%s' % (current_year, curr))
        count = Counter()
        current_year = year

# # do not forget to output the last year if needed!
# if current_year == year:
#     print('%s\t%s' % (current_year, count.most_common(1)))