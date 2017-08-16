import shell
import sys
import os

local_address = shell.run("ifconfig|grep Ethernet -A1|tail -n+2|awk '{print $2}'|cut -d: -f2")

try:
    with open(os.path.expanduser('~/.s4.conf')) as f:
        servers = [
            x
            if x != local_address
            else '0.0.0.0'
            for x in f.read().strip().splitlines()
        ]
except:
    print('~/.s4.conf should contain all server addresses on the local network, one on each line')
    sys.exit(1)

num_servers = len(servers)
