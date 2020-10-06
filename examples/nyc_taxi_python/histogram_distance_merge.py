#!/usr/bin/env pypy3
import sys
import pickle
from ddsketch.ddsketch import DDSketch

sketch = DDSketch()

for line in sys.stdin:
    with open(line.strip(), 'rb') as f:
        sketch.merge(pickle.loads(f.read()))

for i in range(1, 26):
    q = round(i / 100 * 4 - .01, 2)
    print(f'{q},{round(sketch.quantile(q), 2)}')
