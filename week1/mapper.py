#!/usr/bin/python
import sys

findword, filename = sys.argv[1], sys.argv[2]

with open (filename, "r") as myfile:
    print(sum([1 for line in myfile if findword in line]))