#!/bin/bash
set -euo pipefail

if ! which pip || ! which nc || ! which git; then
    pacman --noconfirm -Sy python-pip openbsd-netcat git
fi

if ! which s4-server; then
    (
        cd s4
        python setup.py develop
    )
fi

if [ ! -d xxHash ]; then
    git clone https://github.com/nathants/xxHash
    (
        cd xxHash
        make install
    )
fi
