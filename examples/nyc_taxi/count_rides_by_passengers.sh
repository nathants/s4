#!/bin/bash
set -euo pipefail
cd $(dirname $0)
export TIMEFORMAT=$'%R seconds\n'

bash ./schema.sh

time (
    i=s4://cols # input
    d=s4://tmp  # data
    set -x
    s4 rm -r $d/
    time s4 map-to-n   $i/     $d/01/ 'bcounteachhash | bpartition 1' --regex '_3$'
    time s4 map-from-n $d/01/  $d/02/ 'xargs cat | bsumeachhashu64 | bschema u64:a,u64:a | csv'
    time s4 eval       $d/02/0        'cat | tr , " " | sort -nrk2 | head -n9'
)
