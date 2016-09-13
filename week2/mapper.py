#!/usr/bin/env python

import sys
# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are  you\n")

for line in sys.stdin:
    for word in line.split():
        print('%s\t%s' % (word, 1))