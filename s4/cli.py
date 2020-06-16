#!/usr/bin/env python3
import collections
import pool.thread
import argh
import logging
import os
import requests
import s4
import subprocess
import sys
import tempfile
import time
import util.log
import util.net
import util.retry
import util.strings
import util.time

retry_max_seconds = int(os.environ.get('S4_RETRY_MAX_SECONDS', 60))
retry_exponent = float(os.environ.get('S4_EXPONENT', 1.5))
retry = lambda f: util.retry.retry(f, SystemExit, max_seconds=retry_max_seconds, exponent=retry_exponent)

timeout = s4.timeout * 2 + 60

def rm(prefix, recursive=False):
    _rm(prefix, recursive)

@retry
def _rm(prefix, recursive):
    assert prefix.startswith('s4://')
    if recursive:
        for address, port in s4.servers():
            resp = requests.post(f'http://{address}:{port}/delete?prefix={prefix}&recursive=true', timeout=timeout)
            assert resp.status_code == 200, resp
    else:
        server = s4.pick_server(prefix)
        resp = requests.post(f'http://{server}/delete?prefix={prefix}', timeout=timeout)
        assert resp.status_code == 200, resp

def ls(prefix, recursive=False):
    lines = _ls(prefix, recursive)
    if not lines:
        sys.exit(1)
    return [' '.join(line) for line in lines]

@retry
def _ls(prefix, recursive):
    pool.thread._size = len(s4.servers())
    recursive = 'recursive=true' if recursive else ''
    fs = [pool.thread.submit(requests.get, f'http://{address}:{port}/list?prefix={prefix}&{recursive}', timeout=timeout)
          for address, port in s4.servers()]
    for f in fs:
        assert f.result().status_code == 200, f.result()
    res = [f.result().json() for f in fs]
    return sorted([line for lines in res for line in lines], key=lambda x: x[-1])

def _get_recursive(src, dst):
    bucket, *parts = src.split('s4://')[-1].rstrip('/').split('/')
    prefix = '/'.join(parts) or bucket + '/'
    for line in _ls(src, recursive=True):
        date, time, size, key = line
        token = os.path.dirname(prefix) if dst == '.' else prefix
        path = os.path.join(dst, key.split(token or None)[-1].lstrip(' /'))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cp('s4://' + os.path.join(bucket, key), path)

def _put_recursive(src, dst):
    for dirpath, dirs, files in os.walk(src):
        path = dirpath.split(src)[-1].lstrip('/')
        for file in files:
            cp(os.path.join(dirpath, file), os.path.join(dst, path, file))

def _get(src, dst):
    server = s4.pick_server(src)
    port = util.net.free_port()
    _, temp_path = tempfile.mkstemp(dir='.')
    try:
        server = s4.pick_server(src)
        resp = requests.post(f'http://{server}/prepare_get?key={src}&port={port}', timeout=timeout)
        if resp.status_code == 404:
            sys.exit(1)
        else:
            assert resp.status_code == 200, resp
            uuid = resp.text
            if dst == '-':
                cmd = f'recv {port} | xxh3 --stream'
            else:
                cmd = f'recv {port} | xxh3 --stream > {temp_path}'
            result = s4.run(cmd, stdout=None)
            assert result['exitcode'] == 0, result
            client_checksum = result['stderr']
            resp = requests.post(f'http://{server}/confirm_get?&uuid={uuid}&checksum={client_checksum}', timeout=timeout)
            assert resp.status_code == 200, resp
            if dst.endswith('/'):
                os.makedirs(dst, exist_ok=True)
                dst = os.path.join(dst, os.path.basename(src))
            if dst == '.':
                dst = os.path.basename(src)
            if dst != '-':
                os.rename(temp_path, dst)
    finally:
        with util.exceptions.ignore(FileNotFoundError):
            os.remove(temp_path)

def _put(src, dst):
    if ' ' in src or ' ' in dst:
        logging.info('spaces in keys are not allowed')
        sys.exit(1)
    else:
        if dst.endswith('/'):
            dst = os.path.join(dst, os.path.basename(src))
        server = s4.pick_server(dst)
        server_address = server.split(":")[0]
        resp = requests.post(f'http://{server}/prepare_put?key={dst}', timeout=timeout)
        if resp.status_code == 409:
            logging.info('fatal: key already exists')
            sys.exit(1)
        else:
            assert resp.status_code == 200, resp
            uuid, port = resp.json()
            if src == '-':
                result = s4.run(f'xxh3 --stream | send {server_address} {port}', stdin=sys.stdin)
            else:
                result = s4.run(f'xxh3 --stream < {src} | send {server_address} {port}')
            assert result['exitcode'] == 0, result
            client_checksum = result['stderr']
            resp = requests.post(f'http://{server}/confirm_put?uuid={uuid}&checksum={client_checksum}', timeout=timeout)
            assert resp.status_code == 200, resp

def cp(src, dst, recursive=False):
    _cp(src, dst, recursive)

@retry
def _cp(src, dst, recursive):
    if recursive:
        if src.startswith('s4://'):
            _get_recursive(src, dst)
        elif dst.startswith('s4://'):
            _put_recursive(src, dst)
        else:
            logging.info(f'src or dst needs s4://, got: {src} {dst}')
            sys.exit(1)
    elif src.startswith('s4://') and dst.startswith('s4://'):
        logging.info('there is no move, there is only cp and rm.', file=sys.stderr)
        sys.exit(1)
    elif src.startswith('s4://'):
        _get(src, dst)
    elif dst.startswith('s4://'):
        _put(src, dst)
    else:
        logging.info('src or dst needs s4://')
        sys.exit(1)

def map(indir, outdir, cmd):
    _map(indir, outdir, cmd)

@retry
def _map(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    pool.thread._size = len(s4.servers())
    lines = ls(indir)
    for line in lines:
        assert 'PRE' not in line
        date, time, size, key = line.split()
        assert key.split('/')[-1].isdigit(), f'keys must end with "/[0-9]+" to be colocated, see: s4.pick_server(key). got: {key.split("/")[-1]}'
    b64cmd = util.strings.b64_encode(cmd)
    url = lambda inkey, outkey: f'http://{s4.pick_server(inkey)}/map?inkey={inkey}&outkey={outkey}&b64cmd={b64cmd}'
    fs = {}
    for line in lines:
        date, time, size, key = line.split()
        inkey = os.path.join(indir, key)
        outkey = os.path.join(outdir, key)
        assert s4.pick_server(inkey) == s4.pick_server(outkey)
        f = pool.thread.submit(requests.post, url(inkey, outkey), timeout=timeout)
        fs[f] = inkey, outkey
    with util.time.timeout(s4.timeout):
        while fs:
            f, (inkey, outkey) = fs.popitem()
            resp = f.result()
            if resp.status_code == 429:
                logging.info(f'server busy, retry map: {inkey}')
                f = pool.thread.submit(requests.post, url(inkey, outkey), timeout=timeout)
                fs[f] = inkey, outkey
            elif resp.status_code == 400:
                result = resp.json()
                logging.info('cmd failure')
                logging.info(result['stdout'])
                logging.info(result['stderr'])
                logging.info(f'exitcode={result["exitcode"]}')
                sys.exit(1)
            else:
                assert resp.status_code == 200

def map_to_n(indir, outdir, cmd):
    _map_to_n(indir, outdir, cmd)

@retry
def _map_to_n(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    pool.thread._size = len(s4.servers())
    lines = _ls(indir, recursive=False)
    for line in lines:
        date, time, size, key = line
        assert size != 'PRE'
        assert key.split('/')[-1].isdigit(), f'keys must end with "/[0-9]+" so indir and outdir both live on the same server, see: s4.pick_server(key). got: {key.split("/")[-1]}'
    b64cmd = util.strings.b64_encode(cmd)
    url = lambda inkey, outdir: f'http://{s4.pick_server(inkey)}/map_to_n?inkey={inkey}&outdir={outdir}&b64cmd={b64cmd}'
    fs = {}
    for line in lines:
        date, time, size, key = line
        inkey = os.path.join(indir, key)
        f = pool.thread.submit(requests.post, url(inkey, outdir), timeout=timeout)
        fs[f] = inkey, outdir
    with util.time.timeout(s4.timeout):
        while fs:
            f, (inkey, outdir) = fs.popitem()
            resp = f.result()
            if resp.status_code == 429:
                logging.info(f'server busy, retry map_to_n: {inkey}')
                f = pool.thread.submit(requests.post, url(inkey, outdir), timeout=timeout)
                fs[f] = inkey, outdir
            elif resp.status_code == 400:
                result = resp.json()
                logging.info('cmd failure')
                logging.info(result['stdout'])
                logging.info(result['stderr'])
                logging.info(f'exitcode={result["exitcode"]}')
                sys.exit(1)
            else:
                assert resp.status_code == 200

def map_from_n(indir, outdir, cmd):
    _map_from_n(indir, outdir, cmd)

@retry
def _map_from_n(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    pool.thread._size = len(s4.servers())
    lines = _ls(indir, recursive=True)
    buckets = collections.defaultdict(list)
    bucket, *_ = indir.split('://')[-1].split('/')
    for line in lines:
        date, time, size, key = line
        assert len(key.split('/')) == 3, key
        _indir, _inkey, bucket_num = key.split('/')
        assert bucket_num.isdigit(), f'keys must end with "/[0-9]+" to be colocated, see: s4.pick_server(dir). got: {bucket_num}'
        buckets[bucket_num].append(os.path.join(f's4://{bucket}', key))
    b64cmd = util.strings.b64_encode(cmd)
    url = lambda server, outdir: f'http://{server}/map_from_n?outdir={outdir}&b64cmd={b64cmd}'
    fs = {}
    for bucket_num, inkeys in buckets.items():
        servers = [s4.pick_server(k) for k in inkeys]
        assert len(set(servers)) == 1
        server = servers[0]
        f = pool.thread.submit(requests.post, url(server, outdir), json=inkeys, timeout=timeout)
        fs[f] = server, outdir
    with util.time.timeout(s4.timeout):
        while fs:
            f, (server, outdir) = fs.popitem()
            resp = f.result()
            if resp.status_code == 429:
                logging.info(f'server busy, retry map_to_n: {server}')
                f = pool.thread.submit(requests.post, url(server, outdir), timeout=timeout)
                fs[f] = server, outdir
            elif resp.status_code == 400:
                result = resp.json()
                logging.info('cmd failure')
                logging.info(result['stdout'])
                logging.info(result['stderr'])
                logging.info(f'exitcode={result["exitcode"]}')
                sys.exit(1)
            else:
                assert resp.status_code == 200

if __name__ == '__main__':
    util.log.setup(format='%(message)s')
    argh.dispatch_commands([cp, ls, rm, map, map_to_n, map_from_n])
