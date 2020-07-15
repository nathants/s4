#!/bin/bash
set -euo pipefail
cd $(dirname $0)

source ./schema.sh

s4 rm -r s4://tmp/

time (
    set -x
    time   s4   map-from-n   s4://cols/      s4://tmp/01/   'bzip 2,5 | bschema 7*,8 | bsumeach-hash f64'
    time   s4   map-to-n     s4://tmp/01/    s4://tmp/02/   'bpartition 1'
    time   s4   map-from-n   s4://tmp/02/    s4://tmp/03/   'xargs cat | bsumeach-hash f64 | bschema 7,f64:a | csv'
    time   s4   eval         s4://tmp/03/0                  'tr , " " | sort -nrk2 | head -n9'
)
