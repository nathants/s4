#!/bin/bash
set -euo pipefail

echo; echo nyc taxi dataset is in us-east-1
aws-zones | grep us-east-1

echo; echo cluster health:
s4 health

echo; echo dataset:
prefix='s3://nyc-tlc/trip data/'
keys=$(aws s3 ls "$prefix" \
           | grep yellow \
           | awk '{print $NF}' \
           | while read key; do echo $prefix$key; done)
echo "$keys"

echo; echo headers:
(aws s3 cp "s3://nyc-tlc/trip data/yellow_tripdata_2019-12.csv" - || true) 2>/dev/null | head -n1 | tr , '\n' | cat -n

echo; echo 01 put inputs keys
s4 ls s4://bucket/01 || (
    IFS=$'\n'
    i=0
    for key in $keys; do
        echo $key | s4 cp - s4://bucket/01_inputs/$(printf "%03d" $i)
        i=$((i+1))
    done
)

echo; echo 02 fetch csv and drop header
s4 ls s4://bucket/02 || time s4 map \
                             s4://bucket/01_inputs/ \
                             s4://bucket/02_csv/ \
                             'cat - > url && aws s3 cp "$(cat url)" - | tail -n+2'

echo; echo 03 select, filter, and convert to bsv
s4 ls s4://bucket/03 || time s4 map \
                             s4://bucket/02_csv/ \
                             s4://bucket/03_bsv/ \
                             'cut -d, -f2,3,4,5,10,17 | awk -F, NF==6 | bsv' # fields: 2,3,4,5,10,17 = pickup-date, dropoff-date, passenger-count, distance, payment_type, total_amount

echo; echo 04 groupby passenger count
s4 ls s4://bucket/04 || time s4 map-to-n \
                             s4://bucket/03_bsv/ \
                             s4://bucket/04_grouped/ \
                             'bcut 3 | bbucket 256 | bpartition 256'

echo; echo 05 sort groups
s4 ls s4://bucket/05 || time s4 map \
                             s4://bucket/04_grouped/ \
                             s4://bucket/05_sorted/ \
                             'bsort'

echo; echo 06 merge sorted groups
s4 ls s4://bucket/06 || time s4 map-from-n \
                             s4://bucket/05_sorted/ \
                             s4://bucket/06_merged/ \
                             'bmerge'

echo; echo 07 count and convert to csv
s4 ls s4://bucket/07 || time s4 map \
                             s4://bucket/06_merged/ \
                             s4://bucket/07_counts/ \
                             'bcounteach | bschema *,u64:a | csv'

echo; echo fetch the results; echo
(
    echo num-passengers num-trips
    s4 ls s4://bucket/07 -r \
        | awk '{print $NF}' \
        | while read key; do s4 eval s4://bucket/$key cat; done \
        | grep -v ^$ \
        | tr , ' ' \
        | sort -nrk 2
) | column -t
