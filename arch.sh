#!/bin/bash
set -euo pipefail

if ! which pip || ! which nc || ! which git; then
    pacman --noconfirm -Sy python-pip openbsd-netcat git
fi

cd /mnt

if ! which s4-server; then
    (
        if [ ! -d s4 ]; then
            git clone https://github.com/nathants/s4
        fi
        cd s4
        pip install -r requirements.txt .
    )
fi

if ! which xxhsum; then
    git clone https://github.com/nathants/xxHash
    (
        cd xxHash
        make install
    )
fi
