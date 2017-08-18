import sys
import subprocess

import os

import mmh3

http_port = os.environ.get('http_port', 8000)

max_jobs = 10

def check_output(*a):
    cmd = ' '.join(map(str, a))
    return subprocess.check_output(cmd, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT).decode('utf-8').strip()

_local_address = check_output("ifconfig|grep Ethernet -A1|tail -n+2|awk '{print $2}'|cut -d: -f2")

try:
    with open(os.path.expanduser('~/.s4.conf')) as f:
        servers = [
            x
            if x != _local_address
            else '0.0.0.0'
            for x in f.read().strip().splitlines()
        ]
except:
    print('~/.s4.conf should contain all server addresses on the local network, one on each line')
    sys.exit(1)

_num_servers = len(servers)

def pick_server(dst):
    return servers[mmh3.hash(dst) % _num_servers]
