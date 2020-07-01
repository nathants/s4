#!/bin/bash
set -euo pipefail

echo pcr: passengers count rides

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
(aws s3 cp "s3://nyc-tlc/trip data/yellow_tripdata_2019-12.csv" - || true) 2>/dev/null \
    | head -n1 \
    | sed s/pickup_datetime/pickup_date,pickup_time/ \
    | sed s/dropoff_datetime/dropoff_date,dropoff_time/ \
    | tr , '\n' \
    | cat -n

echo; echo 01 put inputs keys
s4 ls s4://bucket/01 || (
    IFS=$'\n'
    i=0
    for key in $keys; do
        echo $key | s4 cp - s4://bucket/01_inputs/$(printf "%03d" $i)
        i=$((i+1))
    done
)

echo; echo 02 fetch csv, drop header, split date into date and time, filter for 20 columns, and convert to bsv
s4 ls s4://bucket/02 || time s4 map \
                             s4://bucket/01_inputs/ \
                             s4://bucket/02_data/ \
                             'cat - > url && aws s3 cp "$(cat url)" - | tail -n+2 | tr " " , | awk -F, NF==20 | bsv'

echo; echo 03 select passenger-count
s4 ls s4://bucket/pcr/03 || time s4 map \
                             s4://bucket/02_data/ \
                             s4://bucket/pcr/03_selected/ \
                             'bcut 6'

echo; echo 04 bucket and partition
s4 ls s4://bucket/pcr/04 || time s4 map-to-n \
                             s4://bucket/pcr/03_selected/ \
                             s4://bucket/pcr/04_partitioned/ \
                             'bbucket 256 | bpartition 256'

echo; echo 05 sort partitions
s4 ls s4://bucket/pcr/05 || time s4 map \
                             s4://bucket/pcr/04_partitioned/ \
                             s4://bucket/pcr/05_sorted/ \
                             'bsort'

echo; echo 06 merge sorted groups
s4 ls s4://bucket/pcr/06 || time s4 map-from-n \
                             s4://bucket/pcr/05_sorted/ \
                             s4://bucket/pcr/06_merged/ \
                             'bmerge'

echo; echo 07 count and convert to csv
s4 ls s4://bucket/pcr/07 || time s4 map \
                             s4://bucket/pcr/06_merged/ \
                             s4://bucket/pcr/07_counts/ \
                             'bcounteach | bschema *,u64:a | csv'

s4_cat() { s4 eval s4://bucket/$1 cat; }; export -f s4_cat
echo; echo fetch the results; echo
(
    echo num-passengers num-trips
    s4 ls s4://bucket/pcr/07 -r \
        | awk '{print $NF}' \
        | xargs -n1 -P16 -I{} bash -c "s4_cat {}" \
        | grep -v ^$ \
        | tr , ' ' \
        | sort -nrk 2
) | column -t
