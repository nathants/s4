#!/bin/bash
set -euo pipefail

export TIMEFORMAT=$'%R seconds\n'

echo; echo nyc taxi dataset is in us-east-1
aws-zones | grep us-east-1

echo; echo cluster health
s4 health

prefix='s3://nyc-tlc/trip data'

echo; echo put inputs keys
keys=$(aws s3 ls "$prefix/" | grep yellow | awk '{print $NF}' | while read key; do echo "$prefix/$key"; done)
if ! s4 ls s4://inputs; then
    i=0
    echo "$keys" | while read key; do
        num=$(printf "%03d" $i)
        yearmonth=$(echo $key | tr -dc 0-9 | tail -c7)
        echo $key | s4 cp - s4://inputs/${num}_${yearmonth} &
        while (($(jobs | wc -l) > 3 * $(nproc))); do
            sleep .1
        done
        i=$((i+1))
    done
fi
wait

set -x
s4 ls s4://csv || time s4 map s4://inputs/ s4://csv/ 'cat - > url && aws s3 cp "$(cat url)" -'

echo
