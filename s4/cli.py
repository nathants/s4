import sys
import time
import os
import s4
import pool.thread
import argh
import requests


# tmpdir = None

# def _hash(x):
#     return hashlib.sha1(bytes(x, 'utf-8')).hexdigest()

# def _cache_path(key):
#     return '%s/%s' % (tmpdir, _hash(key))

# def _cache_path_prefix(key):
#     return '%s/s3_stubbed_cache.%s.index' % (tmpdir, _hash(key))

# def _prefixes(key):
#     xs = key.split('/')
#     xs = xs[:-1]
#     xs = ['/'.join(xs[:i]) + '/' for i, _ in enumerate(xs, 1)]
#     return [""] + xs

# def rm(url, recursive=False):
#     url = url.split('s3://')[-1]
#     if recursive:
#         try:
#             with open(_cache_path_prefix(url)) as f:
#                 xs = f.read().splitlines()
#         except FileNotFoundError:
#             try:
#                 url = os.path.dirname(url) + '/'
#                 with open(_cache_path_prefix(url)) as f:
#                     xs = [x for x in f.read().splitlines() if x.startswith(url)]
#             except FileNotFoundError:
#                 sys.exit(0)
#         for x in set(xs):
#             rm(x)
#     else:
#         if not os.path.isfile(_cache_path(url)):
#             sys.exit(1)
#         else:
#             os.remove(_cache_path(url))
#             for prefix in _prefixes(url):
#                 with open(_cache_path_prefix(prefix)) as f:
#                     val = '\n'.join([x for x in f.read().splitlines() if x != url])
#                 with open(_cache_path_prefix(prefix), 'w') as f:
#                     f.write(val)

def ls(prefix, recursive=False):
    urls = [f'http://{address}:{port}/list?prefix={prefix}&recursive={"true" if recursive else "false"}' for address, port in s4.servers]
    vals = []
    for url in urls:
        resp = requests.get(url)
        assert resp.status_code == 200
        vals.extend(resp.json())
    vals = [f'_ _ _ {x}' for x in sorted(vals)]
    print(vals)
    return vals

def cp(src, dst, recursive=False):
    if recursive:
        if src.startswith('s3://'):
            bucket, *parts = src.split('s3://')[-1].rstrip('/').split('/')
            prefix = '/'.join(parts)
            for x in ls(src, recursive=True):
                key = x.split()[-1]
                token = os.path.dirname(prefix) if dst == '.' else prefix
                path = os.path.join(dst, key.split(token)[-1].lstrip(' /'))
                os.makedirs(os.path.dirname(path), 0o777, True)
                cp('s3://' + os.path.join(bucket, key), path)
        elif dst.startswith('s3://'):
            for dirpath, dirs, files in os.walk(src):
                path = dirpath.split(src)[-1].lstrip('/')
                for file in files:
                    print(os.path.join(dirpath, file), os.path.join(dst, path, file))
                    cp(os.path.join(dirpath, file), os.path.join(dst, path, file))

    elif src.startswith('s3://') and dst.startswith('s3://'):
        print('mv not implmented yet')
        sys.exit(1)
    elif src.startswith('s3://'):
        if dst == '-':
            print('dont use stdout, python is too slow. use a file path instead.')
        else:
            resp = requests.post(f'http://localhost:{s4.http_port}/new_port')
            assert resp.status_code == 200, resp
            port = int(resp.text)
            temp_path = s4.check_output('mktemp -p .')
            cmd = f'timeout 120 bash -c "set -euo pipefail; nc -q 0 -l {port} | xxhsum > {temp_path}"'
            future = pool.thread.submit(s4.check_output, cmd)
            s4.check_output(f'timeout 120 bash -c "while ! netstat -ln|grep {port}; do sleep .1; done"')
            server = s4.pick_server(src)
            resp = requests.post(f'http://{server}/prepare_get?key={src}&port={port}&server={s4.local_address}')
            assert resp.status_code == 200, resp
            uuid = resp.text
            checksum = future.result()
            resp = requests.post(f'http://{server}/confirm_get?&uuid={uuid}&checksum={checksum}')
            assert resp.status_code == 200, resp
            if dst.endswith('/'):
                s4.check_output('mkdir -p', dst)
                dst = os.path.join(dst, os.path.basename(src))
            s4.check_output('mv', temp_path, dst)

    elif dst.startswith('s3://'):
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
            cmd = f'timeout 120 bash -c "set -euo pipefail; cat {src} | xxhsum | nc {server.split(":")[0]} {port}"'
            checksum = s4.check_output(cmd)
            resp = requests.post(f'http://{server}/confirm_put?uuid={uuid}&checksum={checksum}')
            assert resp.status_code == 200, resp
    else:
        print('something needs s3://')
        sys.exit(1)

def main():
    argh.dispatch_commands([cp,
                            ls])
