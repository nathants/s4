import time
import subprocess
import os
import util.cached
import xxh3

timeout = int(os.environ.get('S4_TIMEOUT', 60 * 10))
conf_path = os.environ.get('S4_CONF_PATH', os.path.expanduser('~/.s4.conf'))

@util.cached.disk_memoize(max_age_seconds=60 * 60 * 24)
def local_addresses():
    vals = {'0.0.0.0', 'localhost', '127.0.0.1'}
    for line in subprocess.check_output("ifconfig").decode().splitlines():
        if ' inet ' in line:
            _, address, *_ = line.split()
            vals.add(address)
    return list(vals)

@util.cached.func
def servers():
    assert os.path.isfile(conf_path), f'conf_path={conf_path} should contain all server addresses on the local network, one on each line'
    with open(conf_path) as f:
        return [(address, port) if address not in local_addresses() else ('0.0.0.0', port)
                for x in f.read().strip().splitlines()
                for address, port in [x.split(':')]]

def run(*a, stdin=None):
    start = time.monotonic()
    proc = subprocess.Popen(
        ' '.join(map(str, a)),
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=stdin or subprocess.DEVNULL,
    )
    stdout = []
    while True:
        assert time.monotonic() - start < timeout
        line = proc.stdout.readline()
        if not line:
            break
        stdout.append(line.decode())
    while True:
        assert time.monotonic() - start < timeout
        exit = proc.poll()
        if exit is not None:
            break
    return {'exitcode': exit,
            'stdout': ''.join(stdout).rstrip(),
            'stderr': proc.stderr.read().decode().rstrip()}

def http_port():
    return [port for address, port in servers() if address == '0.0.0.0'][0]

def on_this_server(key):
    return '0.0.0.0' == pick_server(key).split(':')[0]

def pick_server(url):
    # when path is like s4://bucket/job/worker/001, hash only the last
    # component of the path, in this case: 001. this naming scheme is used for
    # partitioning data, and we want all of the partitions for the same bucket
    # to be on the same server. otherwise hash the whole url.
    assert url.startswith('s4://'), url
    url = url.split('s4://')[-1]
    if url.split('/')[-1].isdigit():
        url = url.split('/')[-1]
    index = xxh3.oneshot_int(url.encode('utf-8')) % len(servers())
    address, port = servers()[index]
    return f'{address}:{port}'
