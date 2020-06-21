#!/bin/bash
set -euo pipefail

# make some data
_gen_bsv ${columns:-8} ${rows:-100000} > data.bsv

# make a unique prefix per worker and invocation
prefix=$(hostnamectl --static)/$(date +%s.%N)

# spam the cluster with data
seq ${size:-100} | xargs -t -n1 -P${workers:-32} -I{} s4 cp data.bsv s4://bucket/$prefix/{}/data.bsv
