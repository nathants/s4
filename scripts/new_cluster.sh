#!/bin/bash
set -eou pipefail

name=$1

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

# make new instances if they don't exist
if ! aws-ec2-ls $name -s running; then
    aws-ec2-new $name \
                --seconds-timeout ${timeout:-$((60*60))} \
                --num ${num:-3} \
                --type ${type:-i3en.large} \
                --ami ${ami:-arch} \
                --spot ${spot:-1} \
                ${extra:-}
fi

# wait for /mnt to come up
for i in {1..1000}; do
    if (($i > 120)); then
        echo fatal: /mnt never came up
        exit 1
    fi
    if aws-ec2-ssh $name -yc 'df | grep /mnt$'; then
        break
    fi
    echo waiting for /mnt
    sleep 1
done

# install s4
for i in {1..1000}; do
    if (($i > 120)); then
        echo fatal: install failed
        exit 1
    fi
    if aws-ec2-ssh $name -yc 'which s4-server || curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash'; then
        break
    fi
    echo retry install
    sleep 1
done

# setup conf
echo "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)" > ~/.s4.conf
aws-ec2-ssh $name -yc 'cat - > ~/.s4.conf' -s "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)"

# restart servers
aws-ec2-ssh $name --no-tty -yc "
    killall -r pypy3 || true
    cd /mnt
    s4-server &>s4.log </dev/null &
"

# make sure all servers are running
aws-ec2-ssh $name -yc 'ps -ef | grep s4-server | grep -v grep'

# echo sshuttle command to access cluster internal network
id=$(aws-ec2-id $name | head -n1)
user=$(aws-ec2-ls $id | grep -Eo "ssh-user=[a-z]+" | cut -d= -f2)
remote=$(aws-ec2-ip $id)
network=$(aws-ec2-ip-private $id|cut -d. -f1)
echo "forward traffic to $name: sshuttle -r $user@$remote $network.0.0.0/8"
echo "wait for ssh on internal address: aws-ec2-wait-for-ssh -yi $name"
