#!/bin/bash
set -eou pipefail

name=$1

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

cd $(dirname $(dirname $0))

# push code
aws-ec2-rsync . :s4/ $name -y

# reinstall s4
aws-ec2-ssh $name -yc "
    cd ~/s4
    sudo python -m pip install -IU -r requirements.txt
    sudo pypy3  -m pip install -IU -r requirements.txt
    sudo python setup.py develop
    sudo pypy3 setup.py develop
"

# kill any running servers
aws-ec2-ssh $name -yc "(ps -ef | grep -e entr -e pypy3 | grep s4-server | grep -v grep | awk '{print \$2}' | xargs kill) || true"

# setup the server reloader
aws-ec2-ssh $name --no-tty -yc "
    cd /mnt
    ((find ~/s4 -type f -name '*.py' -o -name '*.sh' | entr -r s4-server) &> s4.log </dev/null) &
"

# run the local file pusher
find -type f -name '*.py' -o -name '*.sh' | entr aws-ec2-rsync . :s4/ $name -y
