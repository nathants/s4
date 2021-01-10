#!/bin/bash
set -eou pipefail

cd $(dirname $0)

name=$1

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

# make sure an instance profile with s3 read access exists
aws-iam-ensure-instance-profile s3-readonly --policy AmazonS3ReadOnlyAccess

# make new instances if they don't exist. for faster startup use an ami: https://github.com/nathants/bootstraps/blob/master/amis/s4.sh
if ! aws-ec2-ls $name -s running; then
    aws-ec2-new $name \
                --seconds-timeout ${timeout:-$((60*60))} \
                --num ${num:-3} \
                --type ${type:-i3en.xlarge} \
                --ami ${ami:-arch} \
                --profile s3-readonly \
                ${spot:-"--spot"} \
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

# install s4 if needed
for i in {1..1000}; do
    if (($i > 120)); then
        echo fatal: install failed
        exit 1
    fi
    if aws-ec2-ssh $name -yc 'which s4-server || curl -s https://raw.githubusercontent.com/nathants/s4/go/scripts/install_archlinux.sh | bash'; then
        break
    fi
    echo retry install
    sleep 1
done

# setup conf
echo "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)" > ~/.s4.conf
aws-ec2-ssh $name -yc 'cat > ~/.s4.conf' -s "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)"

# restart servers
aws-ec2-ssh $name --no-tty -yc "
    killall -r s4-server || true
    cd /mnt
    s4-server &>s4.log </dev/null &
"

# make sure all servers are running
aws-ec2-ssh $name -yc 'ps -ef | grep s4-server | grep -v grep'

# setup local and cluster conf
bash ./update_conf.sh $name
