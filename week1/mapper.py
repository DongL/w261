#!/usr/bin/python
import sys
import re

for line in sys.stdin:
    # Turn the list into a collection of lowercase words
    for word in re.findall(r'[a-zA-Z]+', line):
        if word.isupper():
            print("Capital 1")
        else:
            print("Lower 1")