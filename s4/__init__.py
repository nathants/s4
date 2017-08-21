import sys
import subprocess

import os

import mmh3

max_jobs = 10

def check_output(*a):
    cmd = ' '.join(map(str, a))
    # print(cmd)
    return subprocess.check_output(cmd, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT).decode('utf-8').strip()
    # return subprocess.check_call(cmd, shell=True, executable='/bin/bash')

local_address = check_output("ifconfig|grep Ethernet -A1|tail -n+2|awk '{print $2}'|cut -d: -f2")

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

def pick_server(dst):
    # when path is like s3://bucket/job/worker/001, hash only the last
    # component of the path. this naming scheme is commonly used for
    # partitioning data, and we want all of the partitions for the same
    # numbered slot to be on the same server. otherwise hash the whole string.
    if dst.split('/')[-1].isdigit():
        dst = dst.split('/')[-1]
    return ':'.join(servers[mmh3.hash(dst) % _num_servers])
