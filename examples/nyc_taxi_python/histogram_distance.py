#!/usr/bin/env pypy3
import sys
import pickle
from ddsketch.ddsketch import DDSketch

header = {}
for i, k in enumerate(sys.stdin.readline().split(',')):
    header[k] = i

distance_keys = [k for k in header if 'distance' in k.lower()]
assert len(distance_keys) == 1, distance_keys
distance_index = header[distance_keys[0]]

bad_lines = 0
sketch = DDSketch()

for line in sys.stdin:
    cols = line.split(',')
    if len(cols) != len(header):
        bad_lines += 1
    else:
        sketch.add(float(cols[distance_index]))

print(f'bad lines: {bad_lines}', file=sys.stderr)
sys.stdout.buffer.write(pickle.dumps(sketch))
