#!/bin/bash
set -euo pipefail
cd $(dirname $0)

source ./schema.sh

s4 rm -r s4://tmp/

time (
    set -x
    time   s4   map          s4://cols/*/*_5   s4://tmp/01/   'btopn 9 f64'
    time   s4   map-from-n   s4://tmp/01/      s4://tmp/02/   'bmerge -r f64'
    time   s4   map-to-n     s4://tmp/02/      s4://tmp/03/   'bpartition 1'
    time   s4   map-from-n   s4://tmp/03/      s4://tmp/04/   'bmerge -r f64 | bhead 9 | bschema f64:a | csv'
    time   s4   eval         s4://tmp/04/0                    'cat'
)
