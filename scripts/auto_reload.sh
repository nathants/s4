#!/bin/bash
set -eou pipefail

cd $(dirname $(dirname $0))

# kill any running servers
aws-ec2-ssh s4-cluster -yc "killall -r pypy3"

# setup the server reloader
aws-ec2-ssh s4-cluster --no-tty -yc "cd /mnt; ((find ~/s4 -type f -name '*.py' -o -name '*.sh' | entr -r s4-server) &> s4.log </dev/null) &"

# run the local file pusher
find -type f -name '*.py' -o -name '*.sh' | entr aws-ec2-rsync . :s4/ s4-cluster -y
