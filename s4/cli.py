#!/usr/bin/env python3
import argh
import logging
import os
import requests
import s4
import shell
import subprocess
import sys
import tempfile
import time
import util.log
import util.net
import util.retry

retry_max_seconds = int(os.environ.get('S4_RETRY_MAX_SECONDS', 60))
retry_exponent = float(os.environ.get('S4_EXPONENT', 1.5))
retry = lambda f: util.retry.retry(f, SystemExit, max_seconds=retry_max_seconds, exponent=retry_exponent)

def rm(prefix, recursive=False):
    _rm(prefix, recursive)

@retry
def _rm(prefix, recursive):
    assert prefix.startswith('s4://')
    if recursive:
        for address, port in s4.servers():
            resp = requests.post(f'http://{address}:{port}/delete?prefix={prefix}&recursive=true')
            assert resp.status_code == 200, resp
    else:
        server = s4.pick_server(prefix)
        resp = requests.post(f'http://{server}/delete?prefix={prefix}')
        assert resp.status_code == 200, resp

def ls(prefix, recursive=False):
    vals = sorted({f'  PRE {x}'
                   if x.endswith('/') else
                   f'_ _ _ {x}'
                   for x in _ls(prefix, recursive)})
    if not vals:
        sys.exit(1)
    return vals

@retry
def _ls(prefix, recursive):
    for address, port in s4.servers():
        url = f'http://{address}:{port}/list?prefix={prefix}&recursive={"true" if recursive else "false"}'
        resp = requests.get(url)
        assert resp.status_code == 200, resp
        yield from resp.json()

def cp(src, dst, recursive=False):
    _cp(src, dst, recursive)

def _cp_from_recursive(src, dst):
    bucket, *parts = src.split('s4://')[-1].rstrip('/').split('/')
    prefix = '/'.join(parts) or bucket + '/'
    for key in _ls(src, recursive=True):
        token = os.path.dirname(prefix) if dst == '.' else prefix
        path = os.path.join(dst, key.split(token or None)[-1].lstrip(' /'))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cp('s4://' + os.path.join(bucket, key), path)

def _cp_to_recursive(src, dst):
    for dirpath, dirs, files in os.walk(src):
        path = dirpath.split(src)[-1].lstrip('/')
        for file in files:
            cp(os.path.join(dirpath, file), os.path.join(dst, path, file))

def _cp_from(src, dst):
    server = s4.pick_server(src)
    port = util.net.free_port()
    _, temp_path = tempfile.mkstemp(dir='.')
    try:
        if dst == '-':
            logging.info('WARN: stdout is potentially slow, consider using a file path instead')
            cmd = f'nc -l {port} | xxh3 --stream'
        else:
            cmd = f'nc -l {port} | xxh3 --stream > {temp_path}'
        start = time.monotonic()
        proc = subprocess.Popen(cmd, shell=True, executable='/bin/bash', stderr=subprocess.PIPE)
        shell.run(s4.cmd_wait_for_port(port))
        server = s4.pick_server(src)
        resp = requests.post(f'http://{server}/prepare_get?key={src}&port={port}')
        assert resp.status_code == 200, resp
        uuid = resp.text
        while proc.poll() is None:
            assert time.monotonic() - start < s4.timeout, f'timeout on cmd: {cmd}'
            time.sleep(.01)
        stderr = proc.stderr.read().decode('utf-8').rstrip()
        assert proc.poll() == 0, stderr
        client_checksum = stderr
        resp = requests.post(f'http://{server}/confirm_get?&uuid={uuid}&checksum={client_checksum}')
        assert resp.status_code == 200, resp
        if dst.endswith('/'):
            os.makedirs(dst, exist_ok=True)
            dst = os.path.join(dst, os.path.basename(src))
        if dst == '.':
            dst = os.path.basename(src)
        if dst != '-':
            os.rename(temp_path, dst)
    finally:
        shell.run('rm -f', temp_path)

def _cp_to(src, dst):
    if dst.endswith('/'):
        dst = os.path.join(dst, os.path.basename(src))
    server = s4.pick_server(dst)
    server_address = server.split(":")[0]
    resp = requests.post(f'http://{server}/prepare_put?key={dst}')
    assert resp.status_code == 200, resp
    uuid, port = resp.json()
    if src == '-':
        logging.info('WARN: stdin is potentially slow, consider using a file path instead')
        cmd = f'xxh3 --stream | nc -N {server_address} {port}'
        result = shell.run(cmd, stdin=sys.stdin, timeout=s4.timeout, warn=True, bufsize=bufsize)
    else:
        cmd = f'xxh3 --stream < {src} | nc -N {server_address} {port}'
        result = shell.run(cmd, timeout=s4.timeout, warn=True, bufsize=bufsize)
    assert result['exitcode'] == 0, result
    client_checksum = result['stderr']
    resp = requests.post(f'http://{server}/confirm_put?uuid={uuid}&checksum={client_checksum}')
    assert resp.status_code == 200, resp

@retry
def _cp(src, dst, recursive):
    if recursive:
        if src.startswith('s4://'):
            _cp_from_recursive(src, dst)
        elif dst.startswith('s4://'):
            _cp_to_recursive(src, dst)
        else:
            logging.info(f'src or dst needs s4://, got: {src} {dst}')
            sys.exit(1)
    elif src.startswith('s4://') and dst.startswith('s4://'):
        logging.info('there is no move, there is only cp and rm.', file=sys.stderr)
        sys.exit(1)
    elif src.startswith('s4://'):
        _cp_from(src, dst)
    elif dst.startswith('s4://'):
        _cp_to(src, dst)
    else:
        logging.info('src or dst needs s4://')
        sys.exit(1)

if __name__ == '__main__':
    util.log.setup(format='%(message)s')
    argh.dispatch_commands([cp, ls, rm])
