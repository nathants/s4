#!/bin/bash
set -xeuo pipefail

if (! which pypy3 || ! which nc || ! which git) &>/dev/null; then
    sudo pacman --noconfirm -Sy \
         git \
         man \
         openbsd-netcat \
         pypy3 \
         python
fi

if ! sudo python3 -m ensurepip; then
    sudo python3 -m ensurepip
fi

if ! sudo pypy3 -m ensurepip; then
    sudo pypy3 -m ensurepip
fi

cd /mnt

(
    if [ ! -d s4 ]; then
        git clone https://github.com/nathants/s4
    fi
    cd s4
    # cli runs python, faster startup
    sudo python3 -m pip install -r requirements.txt
    sudo python3 setup.py develop
    # server runs pypy, faster in general
    sudo pypy3 -m pip install -r requirements.txt
    sudo pypy3 setup.py develop
)

if ! which xxh3 &>/dev/null; then
    git clone https://github.com/nathants/bsv
    (
        cd bsv
        make xxh3
        sudo mv -fv bin/xxh3 /usr/local/bin
    )
fi
