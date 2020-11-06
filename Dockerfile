FROM archlinux:latest

RUN pacman --needed --noconfirm --noprogressbar -Sy \
    git \
    which \
    gcc \
    pypy3 \
    python

ADD s4               /code/s4
ADD setup.py         /code/
ADD requirements.txt /code/

WORKDIR /code

RUN python -m ensurepip && \
    python3 -m pip install -r requirements.txt && \
    python setup.py develop && \
    pypy3 -m ensurepip && \
    pypy3 -m pip install -r requirements.txt && \
    pypy3 setup.py develop

RUN touch ~/.s4.conf

CMD s4 -h; s4-server -h; true
