import sys
import shell
import util.cached
import itertools
import os
import mmh3
import logging
import functools

timeout = int(os.environ.get('S4_TIMEOUT', 60 * 10))
conf_path = os.environ.get('S4_CONF_PATH', os.path.expanduser('~/.s4.conf'))

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

for cmd in ['bash', 'nc', 'xxhsum', 'netstat', 'grep', 'ifconfig']:
    try:
        shell.run('which', cmd)
    except:
        logging.error(f'no such cmd:', cmd)
        sys.exit(1)
assert shell.run('man nc | grep -i bsd'), 'please install the openbsd version of netcat, not gnu netcat'
assert shell.run('echo foo | xxhsum') == 'foo', 'please install this version of xxHash: github.com/nathants/xxHash'
assert shell.run('echo foo | xxhsum', warn=True)['stderr'] == '703c0c8c1824552d', 'please install this version of xxHash: github.com/nathants/xxHash'

local_address = shell.run("ifconfig | grep -o 'inet [^ ]*' | awk '{print $2}' | head -n1")

local_addresses = {
    local_address,
    '0.0.0.0',
    'localhost',
    '127.0.0.1',
}

@util.cached.func
def servers():
    try:
        with open(conf_path) as f:
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
