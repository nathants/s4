import argh
import util.hacks
import os
import pool.thread
import requests
import s4
import sys

if s4._debug:
    def _trace(f):
        def fn(*a, **kw):
            print(*a, kw if kw else '')
            return f(*a, **kw)
        return fn
    requests.post = _trace(requests.post)
    requests.get = _trace(requests.get)

@s4.retry
def rm(prefix, recursive=False):
    assert 's3:' not in prefix
    assert prefix.startswith('s4://')
    if recursive:
        for address, port in s4.servers():
            resp = requests.post(f'http://{address}:{port}/delete?prefix={prefix}&recursive=true')
            assert resp.status_code == 200, resp
    else:
        server = s4.pick_server(prefix)
        resp = requests.post(f'http://{server}/delete?prefix={prefix}')
        assert resp.status_code == 200, resp

@s4.retry
def _ls(prefix, recursive):
    assert 's3:' not in prefix
    for address, port in s4.servers():
        url = f'http://{address}:{port}/list?prefix={prefix}&recursive={"true" if recursive else "false"}'
        resp = requests.get(url)
        assert resp.status_code == 200, resp
        yield from resp.json()

def ls(prefix, recursive=False):
    vals = sorted({f'  PRE {x}'
                   if x.endswith('/') else
                   f'_ _ _ {x}'
                   for x in _ls(prefix, recursive)})
    assert vals
    return vals

@s4.retry
def cp(src, dst, recursive=False):
    assert 's3:' not in src + dst
    if recursive:
        if src.startswith('s4://'):
            bucket, *parts = src.split('s4://')[-1].rstrip('/').split('/')
            prefix = '/'.join(parts)
            for x in ls(src, recursive=True):
                key = x.split()[-1]
                token = os.path.dirname(prefix) if dst == '.' else prefix
                path = os.path.join(dst, key.split(token)[-1].lstrip(' /'))
                os.makedirs(os.path.dirname(path), 0o777, True)
                cp('s4://' + os.path.join(bucket, key), path)
        elif dst.startswith('s4://'):
            for dirpath, dirs, files in os.walk(src):
                path = dirpath.split(src)[-1].lstrip('/')
                for file in files:
                    cp(os.path.join(dirpath, file), os.path.join(dst, path, file))
    elif src.startswith('s4://') and dst.startswith('s4://'):
        print('mv not implmented yet')
        sys.exit(1)
    elif src.startswith('s4://'):
        if dst == '-':
            print('dont use stdout, python is too slow. use a file path instead.')
        else:
            resp = requests.post(f'http://localhost:{s4.http_port()}/new_port')
            assert resp.status_code == 200, resp
            port = int(resp.text)
            temp_path = s4.check_output('mktemp -p .')
            try:
                cmd = f'timeout 120 bash -c "set -euo pipefail; nc -l {port} | xxhsum > {temp_path}"'
                future = pool.thread.submit(s4.check_output, cmd)
                s4.check_output(f'timeout 120 bash -c "while ! netstat -ln|grep {port}; do sleep .1; done"')
                server = s4.pick_server(src)
                resp = requests.post(f'http://{server}/prepare_get?key={src}&port={port}')
                assert resp.status_code == 200, resp
                uuid = resp.text
                checksum = future.result()
                resp = requests.post(f'http://{server}/confirm_get?&uuid={uuid}&checksum={checksum}')
                assert resp.status_code == 200, resp
                if dst.endswith('/'):
                    s4.check_output('mkdir -p', dst)
                    dst = os.path.join(dst, os.path.basename(src))
                s4.check_output('mv', temp_path, dst)
            finally:
                s4.check_output('rm -f', temp_path)
    elif dst.startswith('s4://'):
        if src == '-':
            print('dont use stdin, python is too slow. use a file path instead.')
            sys.exit(1)
        else:
            if dst.endswith('/'):
                dst = os.path.join(dst, os.path.basename(src))
            server = s4.pick_server(dst)
            resp = requests.post(f'http://{server}/prepare_put?key={dst}')
            assert resp.status_code == 200, resp
            uuid, port = resp.json()
            cmd = f'timeout 120 bash -c "set -euo pipefail; cat {src} | xxhsum | nc -N {server.split(":")[0]} {port}"'
            checksum = s4.check_output(cmd)
            resp = requests.post(f'http://{server}/confirm_put?uuid={uuid}&checksum={checksum}')
            assert resp.status_code == 200, resp
    else:
        print('something needs s4://')
        sys.exit(1)

def main():
    if util.hacks.override('--debug'):
        s4._debug = True
    argh.dispatch_commands([cp, ls, rm])
