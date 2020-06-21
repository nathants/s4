#!/bin/bash
set -euo pipefail

if [ ! -f ~/.s4.requirements.done ]; then

    if (! which gcc || ! which pypy3 || ! which nc || ! which git) &>/dev/null; then
        sudo pacman --noconfirm --noprogressbar -Syu
        sudo pacman --needed --noconfirm --noprogressbar -Sy \
             entr \
             gcc \
             git \
             man \
             pypy3 \
             python
    fi

    curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/set_opt.sh | sudo tee /usr/local/bin/set-opt >/dev/null && sudo chmod +x /usr/local/bin/set-opt
    curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/limits.sh | bash

    if ! sudo python -m pip &>/dev/null; then
        sudo python -m ensurepip
    fi

    if ! sudo pypy3 -m pip &>/dev/null; then
        sudo pypy3 -m ensurepip
    fi

    (
        cd ~
        if [ ! -d s4 ]; then
            git clone https://github.com/nathants/s4
        fi
        cd s4
        sudo python -m pip install -r requirements.txt
        sudo pypy3  -m pip install -r requirements.txt
        sudo python setup.py develop
        sudo pypy3  setup.py develop
    )

    (
        cd ~
        if ! which xxh3 &>/dev/null; then
            git clone https://github.com/nathants/bsv
            cd bsv
            make
            sudo mv -fv bin/* /usr/local/bin
        fi
    )

    touch ~/.s4.requirements.done
fi
