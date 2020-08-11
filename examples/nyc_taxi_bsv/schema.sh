#!/bin/bash
set -euo pipefail

export TIMEFORMAT=$'%R seconds\n'

echo; echo nyc taxi dataset is in us-east-1
aws-zones | grep us-east-1

echo; echo cluster health
s4 health

prefix='s3://nyc-tlc/trip data'

echo; echo headers # note: only the first 5 columns are a consistent schema, so we just use those
(aws s3 cp "$prefix/yellow_tripdata_2019-12.csv" - || true) 2>/dev/null | head -n1 | tr , '\n' | head -n5 | cat -n

echo; echo put inputs keys
keys=$(aws s3 ls "$prefix/" | grep yellow | awk '{print $NF}' | while read key; do echo "$prefix/$key"; done)
if ! s4 ls s4://inputs; then
    i=0
    echo "$keys" | while read key; do
        num=$(printf "%03d" $i)
        yearmonth=$(echo $key | tr -dc 0-9 | tail -c6)
        echo $key | s4 cp - s4://inputs/${num}_${yearmonth} &
        while (($(jobs | wc -l) > 3 * $(nproc))); do
            sleep .1
        done
        i=$((i+1))
    done
fi
wait

set -x
s4 ls s4://cols || time s4 map-to-n s4://inputs/ s4://cols/ 'cat > url && aws s3 cp "$(cat url)" - | tail -n+2 | bsv | bschema *,*,*,a:i64,a:f64,... --filter | bunzip $filename'

echo
