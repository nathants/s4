#!/usr/bin/env pypy3
import sys
import collections

header = {}
for i, k in enumerate(sys.stdin.readline().split(',')):
    header[k] = i

total_amount_keys = [k for k in header if 'total' in k.lower()]
assert len(total_amount_keys) == 1, total_amount_keys
total_index = header[total_amount_keys[0]]

pickup_keys = [k for k in header if 'pickup_datetime' in k.lower()]
assert len(pickup_keys) == 1, pickup_keys
pickup_index = header[pickup_keys[0]]

bad_lines = 0
result = collections.defaultdict(float)

for line in sys.stdin:
    cols = line.split(',')
    if len(cols) != len(header):
        bad_lines += 1
    else:
        year_month = cols[pickup_index][:7]
        result[year_month] += float(cols[total_index])

print(f'bad lines: {bad_lines}', file=sys.stderr)
for year_month, sum_total in result.items():
    print(f'{year_month},{int(sum_total)}')
