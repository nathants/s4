#!/usr/bin/env pypy3
import sys
import collections

header = {}
for i, k in enumerate(sys.stdin.readline().split(',')):
    header[k] = i

payment_type_keys = [k for k in header if 'payment_type' in k.lower()]
assert len(payment_type_keys) == 1, payment_type_keys
payment_type_index = header[payment_type_keys[0]]

pickup_keys = [k for k in header if 'pickup_datetime' in k.lower()]
assert len(pickup_keys) == 1, pickup_keys
pickup_index = header[pickup_keys[0]]

bad_lines = 0
result = collections.defaultdict(int)

for line in sys.stdin:
    cols = line.split(',')
    if len(cols) != len(header):
        bad_lines += 1
    else:
        year = cols[pickup_index][:4]
        payment_type = cols[payment_type_index].replace(' ', '_')
        result[(year, payment_type)] += 1

print(f'bad lines: {bad_lines}', file=sys.stderr)
for (year, payment_type), count in result.items():
    print(f'{year},{payment_type},{count}')
