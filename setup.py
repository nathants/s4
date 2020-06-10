import setuptools
import os
import sys
import subprocess
from os import listdir
from os.path import isfile, isdir, basename, dirname, join, abspath

# install deps
setuptools.setup(
    version="0.0.1",
    license='mit',
    name='s4',
    author='nathan todd-stone',
    author_email='me@nathants.com',
    url='http://github.com/nathants/s4',
    description='stupid simple storage service',
    python_requires='>=3.6',
    install_requires=['argh >0.26, <0.27',
                      'requests >2, <3'],
    packages=['s4'],
)

src_path = dirname(abspath(__file__))
dst_path = dirname(abspath(sys.executable))

# install s4 and s4-server
scripts = [
    ('s4/cli.py',    's4'),
    ('s4/server.py', 's4-server'),
]
for src, dst in scripts:
    src = join(src_path, src)
    dst = join(dst_path, dst)
    try:
        os.remove(dst)
    except FileNotFoundError:
        pass
    os.symlink(src, dst)
    os.chmod(dst, 0o775)
    print('link:', dst, '=>', src, file=sys.stderr)

# install send and recv
def cc(*a):
    cmd = ' '.join(map(str, a))
    print(cmd)
    subprocess.check_call(cmd, shell=True, executable='/bin/bash')
scripts = [
    ('s4/send.c', 'send'),
    ('s4/recv.c', 'recv'),
]
for src, dst in scripts:
    src = join(src_path, src)
    dst = join(dst_path, dst)
    cc('gcc -O3 -flto -march=native -mtune=native -o', dst, src)
