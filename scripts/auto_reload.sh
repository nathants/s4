#!/bin/bash
set -eou pipefail

name=$1

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

cd $(dirname $(dirname $0))

# kill any running servers
aws-ec2-ssh $name -yc "killall -r pypy3 || true"
aws-ec2-ssh $name -yc "killall -r entr || true"

# setup the server reloader
aws-ec2-ssh $name --no-tty -yc "cd /mnt; ((find ~/s4 -type f -name '*.py' -o -name '*.sh' | entr -r s4-server) &> s4.log </dev/null) &"

# run the local file pusher
find -type f -name '*.py' -o -name '*.sh' | entr aws-ec2-rsync . :s4/ $name -y
