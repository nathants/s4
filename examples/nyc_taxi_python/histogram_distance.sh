#!/bin/bash
set -euo pipefail
cd $(dirname $0)

source ./schema.sh

aws-ec2-rsync . :bin/ s4-cluster -y
aws-ec2-ssh s4-cluster -yc 'sudo pypy3 -m pip install -q git+https://github.com/DataDog/sketches-py' >/dev/null

s4 rm -r s4://tmp/

time (
    set -x
    time s4   map          s4://csv/      s4://tmp/01/   'pypy3 ~/bin/histogram_distance.py'
    time s4   map-to-n     s4://tmp/01/   s4://tmp/02/   'cat > 0 && echo 0'
    time s4   map-from-n   s4://tmp/02/   s4://tmp/03/   'pypy3 ~/bin/histogram_distance_merge.py'
    time s4   eval         s4://tmp/03/0                 'cat | tr "," " " | column -t | head -n50'
)
