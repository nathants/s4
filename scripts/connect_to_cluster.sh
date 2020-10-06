#!/bin/bash
set -eou pipefail

name=$1

id=$(aws-ec2-id $name | head -n1)
user=$(aws-ec2-ls $id | grep -Eo "ssh-user=[a-z]+" | cut -d= -f2)
remote=$(aws-ec2-ip $id)
network=$(aws-ec2-ip-private $id|cut -d. -f1)

aws-ec2-wait-for-ssh -yi $name

ssh-keyscan $remote >> ~/.ssh/known_hosts 2>/dev/null

which sshuttle || python3 -m pip install sshuttle

sshuttle -r $user@$remote $network.0.0.0/8
