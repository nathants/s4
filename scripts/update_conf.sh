#!/bin/bash
set -eou pipefail

name=$1

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

echo
echo make sure instances are accessible via internal ipv4
aws-ec2-ssh s4-cluster -qiyc whoami >/dev/null

echo
echo update instances confs
aws-ec2-ssh $name -qyc 'cat - > ~/.s4.conf' -s "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)" >/dev/null

echo update local conf
echo "$(aws-ec2-ip-private $name | while read address; do echo $address:8080; done)" > ~/.s4.conf
