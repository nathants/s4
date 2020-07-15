#!/usr/bin/env pypy3
import sys
import collections

result = collections.defaultdict(int)

for line in sys.stdin:
    k, v = line.split(',')
    result[k] += int(v)

for k, v in result.items():
    print(f'{k},{v}')
