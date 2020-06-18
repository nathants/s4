import uuid
import shell
import os
import util.cached
import xxh3

timeout = int(os.environ.get('S4_TIMEOUT', 60 * 5))
conf_path = os.environ.get('S4_CONF_PATH', os.path.expanduser('~/.s4.conf'))

def run(*a, **kw):
    return shell.warn(*a, **kw, timeout=timeout)

def delete(*paths):
    for path in paths:
        with util.exceptions.ignore(FileNotFoundError):
            os.remove(path)

def new_temp_path(dir='.'):
    for _ in range(5):
        temp_path = str(uuid.uuid4())
        temp_path = os.path.join(dir, temp_path)
        temp_path = os.path.abspath(temp_path)
        assert not os.path.isfile(temp_path)
        return temp_path
    assert False

@util.cached.disk_memoize(max_age_seconds=60 * 60 * 24)
def local_addresses():
    vals = {'0.0.0.0', 'localhost', '127.0.0.1'}
    for line in shell.run("ifconfig").splitlines():
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

def http_port():
    return [port for address, port in servers() if address == '0.0.0.0'][0]

def on_this_server(key):
    assert key.startswith('s4://')
    return '0.0.0.0' == pick_server(key).split(':')[0], key

@util.cached.func
def server_num():
    for i, (address, port) in enumerate(servers()):
        if address == '0.0.0.0' and str(port) == str(http_port()):
            return i
    assert False, [servers(), http_port()]

def pick_server(key):
    # when path is like s4://bucket/job/worker/001, hash only the last
    # component of the path, in this case: 001. this naming scheme is used for
    # partitioning data, and we want all of the partitions for the same bucket
    # to be on the same server. otherwise hash the whole key.
    assert key.startswith('s4://'), key
    key = key.split('s4://')[-1]
    if key.split('/')[-1].isdigit():
        key = key.split('/')[-1]
    index = xxh3.oneshot_int(key.encode()) % len(servers())
    address, port = servers()[index]
    return f'{address}:{port}'
