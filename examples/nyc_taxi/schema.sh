#!/bin/bash
set -euo pipefail

echo; echo nyc taxi dataset is in us-east-1
aws-zones | grep us-east-1

echo; echo cluster health
s4 health

echo; echo dataset
prefix='s3://nyc-tlc/trip data/'
keys=$(aws s3 ls "$prefix" | grep yellow | awk '{print $NF}' | while read key; do echo $prefix$key; done)
echo "$keys"

echo; echo headers # note: only the first 5 columns are a consistent schema, so we just use those
(aws s3 cp "s3://nyc-tlc/trip data/yellow_tripdata_2019-12.csv" - || true) 2>/dev/null | head -n1 | tr , '\n' | head -n5 | cat -n

echo; echo put inputs keys
if ! s4 ls s4://inputs; then
    i=0
    echo "$keys" | while read key; do
        echo $key | s4 cp - s4://inputs/$(printf "%03d" $i)
        i=$((i+1))
    done
fi

set -x
s4 ls s4://bsv  || time s4 map      s4://inputs/ s4://bsv/  'cat - > url && aws s3 cp "$(cat url)" - | tail -n+2 | bsv | blz4'
s4 ls s4://cols || time s4 map-to-n s4://bsv/    s4://cols/ 'blz4d | bschema *,*,*,a:u64,a:f64,... --filter | bunzip $filename'

echo
