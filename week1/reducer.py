#!/usr/bin/python
import sys

total = 1
current_word = None

for countStr in sys.stdin:
    word, count = countStr.split()
    if current_word == word:
        total += int(count)
    elif current_word is None:
        current_word = word
    else:
        print(current_word, total)
        current_word = word
        total = 1
print(current_word, total)