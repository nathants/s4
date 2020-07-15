#!/usr/bin/env pypy3
import sys
import collections

result = collections.defaultdict(int)

for line in sys.stdin:
    year, payment_type, count = line.split(',')
    result[(year, payment_type)] += int(count)

for (year, payment_type), count in result.items():
    print(f'{year},{payment_type},{count}')
