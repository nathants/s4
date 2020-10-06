#!/usr/bin/env pypy3
import sys
import collections

result = collections.defaultdict(int)

for line in sys.stdin:
    result[len(line.split(','))] += 1

for k, v in result.items():
    print(f'{k},{v}')
