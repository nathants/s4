import hashlib
import functools
import shutil
import os
import shell
import util.cached
import util.exceptions
import uuid
import traceback
import logging

conf_path   = os.environ.get('S4_CONF_PATH', os.path.expanduser('~/.s4.conf'))
timeout = int(os.environ.get('S4_TIMEOUT', 60 * 5))
max_timeout = timeout * 2 + 15 # one timeout for fifo queue on server, one timeout for the job once it starts, + grace period

def run(*a, timeout=timeout, **kw):
    return shell.warn(*a, **kw, timeout=timeout)

def delete_dirs(dirs):
    for dir in dirs:
        shutil.rmtree(dir, ignore_errors=True)

def delete(*paths):
    for path in paths:
        with util.exceptions.ignore(FileNotFoundError):
            os.remove(path)

def new_temp_path(dir):
    for _ in range(5):
        temp_path = str(uuid.uuid4())
        temp_path = os.path.join(dir, temp_path)
        temp_path = os.path.abspath(temp_path)
        if not os.path.isfile(temp_path):
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
    server, port = pick_server(key).split(':')
    return server == '0.0.0.0' and port == str(http_port())

def key_prefix(key):
    key = key.split('/')[-1]
    prefix = key.split('_')[0]
    if prefix.isdigit():
        return prefix
    else:
        return key

def key_suffix(key):
    if not key_prefix(key).isdigit():
        return None
    try:
        return key.split('/')[-1].split('_', 1)[1]
    except IndexError:
        return None

def suffix(keys):
    suffixes = [key_suffix(key) for key in keys]
    if all(suffixes) and len(set(suffixes)) == 1:
        return f'_{suffixes[0]}'
    else:
        return ''

def hash(val):
    return int.from_bytes(hashlib.blake2s(val.encode()).digest()[:8], "little", signed=False)

def pick_server(key):
    assert not key.endswith('/'), key
    assert key.startswith('s4://'), key
    prefix = key_prefix(key)
    try:
        val = int(prefix)
    except ValueError:
        val = hash(prefix)
    index = val % len(servers())
    address, port = servers()[index]
    return f'{address}:{port}'

def return_stacktrace(decoratee):
    @functools.wraps(decoratee)
    async def decorated(*a, **kw):
        try:
            return await decoratee(*a, **kw)
        except:
            logging.exception(f'failure: {decoratee}')
            return {'code': 500, 'body': traceback.format_exc()}
    return decorated
