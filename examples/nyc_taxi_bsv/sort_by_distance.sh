#!/bin/bash
set -euo pipefail
cd $(dirname $0)

source ./schema.sh

s4 rm -r s4://tmp/

time (
    set -x
    time   s4   map          s4://cols/*/*_5   s4://tmp/01/   'bsort -r f64'
    time   s4   map-from-n   s4://tmp/01/      s4://tmp/02/   'bmerge -r f64'
    time   s4   map-to-n     s4://tmp/02/      s4://tmp/03/   'bpartition -l 1'
    time   s4   map-from-n   s4://tmp/03/      s4://tmp/04/   'bmerge -lr f64 | blz4'
)

time   s4   eval   s4://tmp/04/0   'blz4d | xxhsum'
time   s4   eval   s4://tmp/04/0   'blz4d | bschema f64:a | csv | head'
time   s4   eval   s4://tmp/04/0   'blz4d | bcountrows | bschema i64:a | csv'
