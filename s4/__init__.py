import sys
import util.cached
import itertools
import subprocess
import os
import mmh3
import logging
import functools

def retry(f):
    @functools.wraps(f)
    def fn(*a, **kw):
        for i in itertools.count():
            try:
                return f(*a, **kw)
            except Exception as e:
                if i == 6:
                    raise
                logging.info(f'retrying: {f.__module__}.{f.__name__}, because of: {type(e)} {e}')
    return fn

_debug = False

def check_output(*a):
    cmd = ' '.join(map(str, a))
    if _debug:
        print(cmd, flush=True)
    return subprocess.check_output(cmd, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT).decode('utf-8').strip()

def check_call(*a):
    cmd = ' '.join(map(str, a))
    if _debug:
        print(cmd, flush=True)
    return subprocess.check_call(cmd, shell=True, executable='/bin/bash')

for cmd in ['timeout', 'bash', 'nc', 'xxhsum', 'netstat', 'grep']:
    try:
        check_call('which', cmd, '&>/dev/null')
    except:
        logging.error(f'no such cmd:', cmd)
        sys.exit(1)
assert check_output('man nc | grep -i bsd'), 'please install the openbsd version of netcat, not gnu netcat'
assert check_output('echo foo | xxhsum 2>/dev/null') == 'foo', 'please install this version of xxHash: github.com/nathants/xxHash'
assert check_output('echo foo | xxhsum 1>/dev/null') == '703c0c8c1824552d', 'please install this version of xxHash: github.com/nathants/xxHash'

local_address = check_output("ifconfig|grep Ethernet -A1|grep addr:|head -n1|awk '{print $2}'|cut -d: -f2")

local_addresses = {
    local_address,
    '0.0.0.0',
    'localhost',
    '127.0.0.1',
}

_conf_path = os.path.expanduser('~/.s4.conf')

@util.cached.func
def servers():
    try:
        with open(_conf_path) as f:
            return [(address, port)
                    if address not in local_addresses
                    else ('0.0.0.0', port)
                    for x in f.read().strip().splitlines()
                    for address, port in [x.split(':')]]
    except:
        print('~/.s4.conf should contain all server addresses on the local network, one on each line', file=sys.stderr)
        sys.exit(1)

def http_port():
    return [port for address, port in servers() if address in local_addresses][0]

def pick_server(s3_url):
    # when path is like s4://bucket/job/worker/001, hash only the last
    # component of the path. this naming scheme is commonly used for
    # partitioning data, and we want all of the partitions for the same
    # numbered slot to be on the same server. otherwise hash the whole string.
    s3_url = s3_url.split('s4://')[-1]
    if s3_url.split('/')[-1].isdigit():
        s3_url = s3_url.split('/')[-1]
    return ':'.join(servers()[mmh3.hash(s3_url) % len(servers())])
