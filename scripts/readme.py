#!/usr/bin/env python3
import os
import subprocess

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
co = lambda *a: subprocess.check_output(' '.join(map(str, a)), shell=True, executable='/bin/bash').decode('utf-8').strip()

with open('readme.md') as f:
    xs = f.read().splitlines()

before = []
for x in xs:
    before.append(x)
    if x.startswith('## api'):
        before.append('')
        break

after = []
for line in co('s4 -h | tail -n+5 | head -n-3').splitlines():
    try:
        name, description = line.split(None, 1)
    except:
        print([line])
        raise
    name = f's4 {name}'
    usage = co(f'{name} -h')
    before.append(f'- [{name}](#{name.replace(" ", "-")}) - {description}'.strip())
    after.append(f'\n### [{name}](https://github.com/nathants/s4/search?l=Python&q="def+{name.split()[-1]}")\n```\n{usage.rstrip()}\n```')

with open('readme.md', 'w') as f:
    f.write('\n'.join(before + ['\n## usage'] + after) + '\n')
