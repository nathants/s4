#!/bin/bash
set -euo pipefail

false # pypy setup not working on bionic, weird cffi issue. use arch instead

if ! which pypy3 || ! which nc || ! which git; then
    sudo add-apt-repository -y ppa:pypy/ppa
    sudo apt-get install -y python3-pip python3-setuptools git build-essential pypy3 virtualenv
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
        sudo pypy3-pip install -r requirements.txt
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
