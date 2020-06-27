#!/usr/bin/env python3
import argh
import collections
import concurrent.futures
import json
import logging
import os
import pool.thread
import s4
import shell
import sys
import urllib.error
import urllib.request
import util.log
import util.net
import util.retry
import util.strings
import util.time
from pool.thread import submit

retry_max_seconds = int(os.environ.get('S4_RETRY_MAX_SECONDS', 60))
retry_exponent = float(os.environ.get('S4_EXPONENT', 1.5))
_retry = lambda f: util.retry.retry(f, SystemExit, max_seconds=retry_max_seconds, exponent=retry_exponent)

def _http_post(url, data='', timeout=s4.max_timeout):
    try:
        with urllib.request.urlopen(url, data.encode(), timeout=timeout) as resp:
            body = resp.read().decode()
            code = resp.status
    except urllib.error.HTTPError as e:
        return {'body': e.msg, 'code': e.code}
    else:
        return {'body': body, 'code': code}

def _http_get(url, timeout=s4.max_timeout):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode()
            code = resp.status
    except urllib.error.HTTPError as e:
        return {'body': e.msg, 'code': e.code}
    else:
        return {'body': body, 'code': code}

def rm(prefix, recursive=False):
    assert prefix.startswith('s4://')
    _rm(prefix, recursive)

@_retry
def _rm(prefix, recursive):
    if recursive:
        for address, port in s4.servers():
            resp = _http_post(f'http://{address}:{port}/delete?prefix={prefix}&recursive=true')
            assert resp['code'] == 200, resp
    else:
        server = s4.pick_server(prefix)
        resp = _http_post(f'http://{server}/delete?prefix={prefix}')
        assert resp['code'] == 200, resp

def eval(key, cmd):
    resp = _http_post(f'http://{s4.pick_server(key)}/eval?key={key}&b64cmd={util.strings.b64_encode(cmd)}')
    if resp['code'] == 404:
        sys.exit(1)
    else:
        print(resp['body'])

@argh.arg('prefix', nargs='?', default=None)
def ls(prefix, recursive=False):
    assert not prefix or prefix.startswith('s4://'), 'fatal: prefix must start with s4://'
    if prefix:
        lines = _ls(prefix, recursive)
    else:
        lines = _ls_buckets()
    assert lines
    just = max(len(size) for date, time, size, path in lines)
    for date, time, size, path in lines:
        print(date.ljust(10), time.ljust(8), size.rjust(just), path)

@_retry
def _ls(prefix, recursive):
    recursive = 'recursive=true' if recursive else ''
    fs = [submit(_http_get, f'http://{address}:{port}/list?prefix={prefix}&{recursive}') for address, port in s4.servers()]
    for f in fs:
        assert f.result()['code'] == 200, f.result()
    res = [json.loads(f.result()['body']) for f in fs]
    return sorted(set(tuple(line) for lines in res for line in lines), key=lambda x: x[-1])

@_retry
def _ls_buckets():
    fs = [submit(_http_get, f'http://{address}:{port}/list_buckets') for address, port in s4.servers()]
    for f in fs:
        assert f.result()['code'] == 200, f.result()
    buckets = {}
    for f in fs:
        for date, time, size, path in json.loads(f.result()['body']):
            buckets[path] = date, time, size, path
    return [buckets[path] for path in sorted(buckets)]

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
    temp_path = s4.new_temp_path()
    try:
        server = s4.pick_server(src)
        resp = _http_post(f'http://{server}/prepare_get?key={src}&port={port}')
        if resp['code'] == 404:
            sys.exit(1)
        else:
            assert resp['code'] == 200, resp
            uuid = resp['body']
            if dst == '-':
                cmd = f'recv {port} | xxh3 --stream'
            else:
                assert not os.path.isfile(temp_path)
                cmd = f'recv {port} | xxh3 --stream > {temp_path}'
            result = s4.run(cmd, stdout=None)
            assert result['exitcode'] == 0, result
            client_checksum = result['stderr']
            resp = _http_post(f'http://{server}/confirm_get?&uuid={uuid}&checksum={client_checksum}')
            assert resp['code'] == 200, resp
            if dst.endswith('/'):
                os.makedirs(dst, exist_ok=True)
                dst = os.path.join(dst, os.path.basename(src))
            if dst == '.':
                dst = os.path.basename(src)
            if dst != '-':
                os.rename(temp_path, dst)
    finally:
        s4.delete(temp_path)

def _put(src, dst):
    if dst.endswith('/'):
        dst = os.path.join(dst, os.path.basename(src))
    server = s4.pick_server(dst)
    server_address = server.split(":")[0]
    resp = _http_post(f'http://{server}/prepare_put?key={dst}')
    if resp['code'] == 409:
        logging.info('fatal: key already exists')
        sys.exit(1)
    else:
        assert resp['code'] == 200, resp
        uuid, port = json.loads(resp['body'])
        if src == '-':
            result = s4.run(f'xxh3 --stream | send {server_address} {port}', stdin=sys.stdin)
        else:
            result = s4.run(f'< {src} xxh3 --stream | send {server_address} {port}')
        assert result['exitcode'] == 0, result
        client_checksum = result['stderr']
        resp = _http_post(f'http://{server}/confirm_put?uuid={uuid}&checksum={client_checksum}')
        assert resp['code'] == 200, resp

def cp(src, dst, recursive=False):
    assert not (src.startswith('s4://') and dst.startswith('s4://')), 'fatal: there is no move, there is only cp and rm.'
    assert ' ' not in src and ' ' not in dst, 'fatal: spaces in keys are not allowed'
    assert not dst.startswith('s4://') or not dst.split('s4://')[-1].startswith('_'), 'fatal: buckets cannot start with underscore'
    assert not src.startswith('s4://') or not src.split('s4://')[-1].startswith('_'), 'fatal: buckets cannot start with underscore'
    _cp(src, dst, recursive)

@_retry
def _cp(src, dst, recursive):
    if recursive:
        if src.startswith('s4://'):
            _get_recursive(src, dst)
        elif dst.startswith('s4://'):
            _put_recursive(src, dst)
        else:
            logging.info(f'fatal: src or dst needs s4://, got: {src} {dst}')
            sys.exit(1)
    elif src.startswith('s4://'):
        _get(src, dst)
    elif dst.startswith('s4://'):
        _put(src, dst)
    else:
        assert False, 'fatal: src or dst needs s4://'

def _post_all_retrying_429(urls):
    pool.thread._size = max(len(urls), 128)
    pool.thread._pool.clear_cache()
    try:
        fs = {submit(_http_post, url, data): url for url, data in urls}
    except ValueError:
        fs = {submit(_http_post, url): url for url in urls}
    with util.time.timeout(s4.max_timeout):
        while fs:
            f = next(iter(fs))
            url = fs.pop(f)
            resp = f.result()
            if resp['code'] == 429:
                logging.info(f'server busy, retry url: {url}')
                try:
                    url, data = url
                except ValueError:
                    fs[submit(_http_post, url)] = url
                else:
                    fs[submit(_http_post, url, data)] = url
            elif resp['code'] == 400:
                result = json.loads(resp['body'])
                logging.info('fatal: cmd failure')
                logging.info(result['stdout'])
                logging.info(result['stderr'])
                logging.info(f'exitcode={result["exitcode"]}')
                sys.exit(1)
            else:
                logging.info(url)
                assert resp['code'] == 200

def map(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    _map(indir, outdir, cmd)

@_retry
def _map(indir, outdir, cmd):
    lines = _ls(indir, recursive=True)
    urls = []
    proto, path = indir.split('://')
    bucket, path = path.split('/', 1)
    for line in lines:
        date, time, size, key = line
        key = key.split(path)[-1]
        if size == 'PRE':
            continue
        assert key.split('/')[-1].isdigit(), f'keys must end with "/[0-9]+" to be colocated, see: s4.pick_server(key). got: {key.split("/")[-1]}'
        inkey = os.path.join(indir, key)
        outkey = os.path.join(outdir, key)
        assert s4.pick_server(inkey) == s4.pick_server(outkey)
        urls.append(f'http://{s4.pick_server(inkey)}/map?inkey={inkey}&outkey={outkey}&b64cmd={util.strings.b64_encode(cmd)}')
    _post_all_retrying_429(urls)

def map_to_n(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    _map_to_n(indir, outdir, cmd)

@_retry
def _map_to_n(indir, outdir, cmd):
    lines = _ls(indir, recursive=False)
    urls = []
    for line in lines:
        date, time, size, key = line
        assert size != 'PRE', key
        assert key.split('/')[-1].isdigit(), f'keys must end with "/[0-9]+" so indir and outdir both live on the same server, see: s4.pick_server(key). got: {key.split("/")[-1]}'
        inkey = os.path.join(indir, key)
        urls.append(f'http://{s4.pick_server(inkey)}/map_to_n?inkey={inkey}&outdir={outdir}&b64cmd={util.strings.b64_encode(cmd)}')
    _post_all_retrying_429(urls)

def map_from_n(indir, outdir, cmd):
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    _map_from_n(indir, outdir, cmd)

@_retry
def _map_from_n(indir, outdir, cmd):
    lines = _ls(indir, recursive=True)
    buckets = collections.defaultdict(list)
    bucket, *_ = indir.split('://')[-1].split('/')
    for line in lines:
        date, time, size, key = line
        assert len(key.split('/')) == 3, key
        _indir, _inkey, bucket_num = key.split('/')
        assert bucket_num.isdigit(), f'keys must end with "/[0-9]+" to be colocated, see: s4.pick_server(dir). got: {bucket_num}'
        buckets[bucket_num].append(os.path.join(f's4://{bucket}', key))
    urls = []
    for bucket_num, inkeys in buckets.items():
        servers = [s4.pick_server(k) for k in inkeys]
        assert len(set(servers)) == 1
        server = servers[0]
        urls.append((f'http://{server}/map_from_n?outdir={outdir}&b64cmd={util.strings.b64_encode(cmd)}', json.dumps(inkeys)))
    _post_all_retrying_429(urls)

def servers():
    return [':'.join(x) for x in s4.servers()]

def health():
    fail = False
    fs = {}
    for addr, port in s4.servers():
        f = submit(_http_get, f'http://{addr}:{port}/health', timeout=2)
        fs[f] = addr, port
    for f in concurrent.futures.as_completed(fs):
        addr, port = fs[f]
        try:
            resp = f.result()
        except:
            fail = True
            print(f'unhealthy: {addr}:{port}')
        else:
            if resp['code'] == 200:
                print(f'healthy:   {addr}:{port}')
            else:
                fail = True
                print(f'unhealthy: {addr}:{port}')
    if fail:
        sys.exit(1)

if __name__ == '__main__':
    util.log.setup(format='%(message)s')
    pool.thread._size = len(s4.servers())
    try:
        shell.dispatch_commands(globals(), __name__)
    except AssertionError as e:
        if e.args:
            logging.info(util.colors.red(e.args[0]))
        sys.exit(1)
