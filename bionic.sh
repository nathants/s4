#!/bin/bash
set -euo pipefail

if ! which pip || ! which nc || ! which git; then
    sudo apt-get update
    sudo apt-get install -y python3-pip python3-setuptools git build-essential
    sudo ln -sf /usr/bin/python3 /usr/local/bin/python
    sudo ln -sf /usr/bin/pip3 /usr/local/bin/pip
fi

cd /mnt

if ! which s4-server; then
    (
        if [ ! -d s4 ]; then
            git clone https://github.com/nathants/s4
        fi
        cd s4
        sudo pip install -r requirements.txt
        sudo python setup.py develop
    )
fi

if ! which xxhsum; then
    git clone https://github.com/nathants/xxHash
    (
        cd xxHash
        sudo make install
    )
fi
