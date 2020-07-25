#!/bin/bash
set -euo pipefail

sudo pacman --needed --noconfirm --noprogressbar -Sy \
     entr \
     gcc \
     git \
     man \
     pypy3 \
     python

curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/set_opt.sh | sudo tee /usr/local/bin/set-opt >/dev/null && sudo chmod +x /usr/local/bin/set-opt
curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/limits.sh | bash

curl -s https://raw.githubusercontent.com/nathants/bsv/master/scripts/install_archlinux.sh | bash

sudo python -m ensurepip
sudo pypy3 -m ensurepip

cd ~
sudo rm -rf s4
git clone https://github.com/nathants/s4
cd s4
sudo python -m pip install -r requirements.txt
sudo pypy3  -m pip install -r requirements.txt
sudo python setup.py develop
sudo pypy3  setup.py develop
