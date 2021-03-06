#!/bin/bash
set -euo pipefail

cd $(dirname $0)

if ! which aws-ec2-new &>/dev/null; then
    echo fatal: need to install https://github.com/nathants/cli-aws
    exit 1
fi

export type=${type:-i3en.large}
export zone=${zone:-$(aws-ec2-max-spot-price $type 2>/dev/null | head -n1 | cut -d" " -f1)}
export extra="--zone $zone"
echo zone=$zone type=$type

if ! aws-ec2-ls s4-load-testers -s running; then
    num=${num_cluster:-3} bash new_cluster.sh s4-cluster & cluster=$!
    num=${num_testers:-5} bash new_cluster.sh s4-load-testers & testers=$!
    trap "kill $cluster $testers &>/dev/null" EXIT
    wait $cluster $testers
    tempdir=$(mktemp -d)
    trap "rm -rf $tempdir" EXIT
    (
        cd $tempdir
        aws-ec2-scp :.s4.conf . s4-cluster -y
        aws-ec2-scp .s4.conf  : s4-load-testers -y
    )
fi

# run the load test
aws-ec2-ssh s4-load-testers -qyc "
    _gen_csv ${columns:-8} ${rows:-1000} > data.csv
    time seq ${size:-10000} | xargs -t -n1 -P${workers:-32} -I{} s4 cp data.csv s4://bucket/\$(hostnamectl --static)/\$(date +%s.%N)/{} &>/dev/null
" 2>/dev/null | grep real
