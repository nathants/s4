import logging
import os
import shell
import sys
import util.cached
import xxh3

timeout = int(os.environ.get('S4_TIMEOUT', 60 * 10))
conf_path = os.environ.get('S4_CONF_PATH', os.path.expanduser('~/.s4.conf'))

for cmd in ['timeout', 'bash', 'nc', 'xxh3', 'ss', 'grep', 'ifconfig']:
    try:
        shell.run('which', cmd)
    except:
        logging.error(f'no such cmd: {cmd}')
        sys.exit(1)
assert shell.run('echo foo | xxh3 --stream') == 'foo', 'please install this version of xxh3: github.com/nathants/bsv'
assert shell.run('echo foo | xxh3 --stream', warn=True)['stderr'] == '9f15a20cf20cea24', 'please install this version of xxh3: github.com/nathants/bsv'

local_addresses = {'0.0.0.0',
                   'localhost',
                   '127.0.0.1'}
for address in shell.run("ifconfig | grep -o 'inet [^ ]*' | cut -d' ' -f2").splitlines():
    local_addresses.add(address)

def cmd_wait_for_port(port):
    return f'timeout {timeout} bash -c "while ! ss -tlH | grep :{port}; do sleep .1; done"'

@util.cached.func
def servers():
    try:
        with open(conf_path) as f:
            return [(address, port) if address not in local_addresses else ('0.0.0.0', port)
                    for x in f.read().strip().splitlines()
                    for address, port in [x.split(':')]]
    except:
        logging.info('~/.s4.conf should contain all server addresses on the local network, one on each line')
        sys.exit(1)

def http_port():
    return [port for address, port in servers() if address == '0.0.0.0'][0]

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
