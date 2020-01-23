#!/bin/bash
set -euo pipefail

if ! which pypy3 || ! which nc || ! which git; then
    sudo pacman --noconfirm -Sy python-pip openbsd-netcat git pypy3 python-virtualenv man
    virtualenv --python=pypy3 ~/.env/pypy3
    sudo ln -sfv ~/.env/pypy3/bin/python3 /usr/local/bin/pypy3
    sudo ln -sfv ~/.env/pypy3/bin/pip3 /usr/local/bin/pypy3-pip
fi

cd /mnt

if ! which s4-server; then
    (
        if [ ! -d s4 ]; then
            git clone https://github.com/nathants/s4
        fi
        cd s4
        sudo pypy3-pip install -r requirements.txt # server runs pypy
        sudo pip install -r requirements.txt # cli runs python, faster startup
        sudo pypy3 setup.py develop
    )
fi

if ! which xxh3; then
    git clone https://github.com/nathants/bsv
    (
        cd bsv
        make xxh3
        sudo mv -fv bin/xxh3 /usr/local/bin
    )
fi
