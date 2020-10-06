#!/usr/bin/env pypy3
import sys
import collections

result = collections.defaultdict(float)

for line in sys.stdin:
    year_month, sum_total = line.split(',')
    result[year_month] += float(sum_total)

for year_month, sum_total in result.items():
    print(f'{year_month},{sum_total}')
