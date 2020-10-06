#!/bin/bash
set -euo pipefail

sudo pacman --needed --noconfirm --noprogressbar -Sy \
     entr \
     gcc \
     git \
     man \
     go \
     pypy3 \
     python

curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/limits.sh | bash

curl -s https://raw.githubusercontent.com/nathants/bsv/master/scripts/install_archlinux.sh | bash

sudo python -m ensurepip
sudo python -m pip install --progress-bar off awscli

cd ~
sudo rm -rf s4
git clone https://github.com/nathants/s4
cd s4
git checkout go
make -j
sudo mv -fv bin/* /usr/local/bin/
