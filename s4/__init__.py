import sys
import itertools
import subprocess
import os
import mmh3
import logging

max_jobs = 10

def retry(f):
    def fn(*a, **kw):
        for i in itertools.count():
            try:
                return f(*a, **kw)
            except Exception as e:
                if i == 6:
                    raise
                logging.info('retrying: %s.%s, because of: %s', f.__module__, f.__name__, e)
    return fn

def check_output(*a):
    cmd = ' '.join(map(str, a))
    # print(cmd)
    return subprocess.check_output(cmd, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT).decode('utf-8').strip()

def check_call(*a):
    cmd = ' '.join(map(str, a))
    # print(cmd)
    return subprocess.check_call(cmd, shell=True, executable='/bin/bash')

local_address = check_output("ifconfig|grep Ethernet -A1|grep addr:|head -n1|awk '{print $2}'|cut -d: -f2")

local_addresses = {
    local_address,
    '0.0.0.0',
    'localhost',
    '127.0.0.1',
}

try:
    with open(os.path.expanduser('~/.s4.conf')) as f:
        servers = [(address, port)
                   if address not in local_addresses
                   else ('0.0.0.0', port)
                   for x in f.read().strip().splitlines()
                   for address, port in [x.split(':')]]
except:
    print('~/.s4.conf should contain all server addresses on the local network, one on each line')
    sys.exit(1)

_num_servers = len(servers)

try:
    http_port = [port for address, port in servers if address in local_addresses][0]
except IndexError:
    http_port = None

def pick_server(s3_url):
    # when path is like s4://bucket/job/worker/001, hash only the last
    # component of the path. this naming scheme is commonly used for
    # partitioning data, and we want all of the partitions for the same
    # numbered slot to be on the same server. otherwise hash the whole string.
    s3_url = s3_url.split('s4://')[-1]
    if s3_url.split('/')[-1].isdigit():
        s3_url = s3_url.split('/')[-1]
    return ':'.join(servers[mmh3.hash(s3_url) % _num_servers])
