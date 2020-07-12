#!/bin/bash
set -euo pipefail
cd $(dirname $0)

source ./schema.sh

s4 rm -r s4://tmp/

time (
    set -x
    time   s4   map-to-n     s4://cols/*/*_5   s4://tmp/01/   'bsplit 100'
    time   s4   map          s4://tmp/01/      s4://tmp/02/   'bsort i64'
    time   s4   map-from-n   s4://tmp/02/      s4://tmp/03/   'brmerge i64'
    time   s4   map-to-n     s4://tmp/03/      s4://tmp/04/   'bpartition 1'
    time   s4   map-from-n   s4://tmp/04/      s4://tmp/05/   'brmerge i64 | bschema i64:a | csv'
    time   s4   eval         s4://tmp/05/0                    'head -n9'
)
