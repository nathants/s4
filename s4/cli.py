#!/usr/bin/env python3
import argh
import collections
import concurrent.futures
import fnmatch
import json
import logging
import os
import pool.thread
import pprint
import s4
import shell
import sys
import urllib.error
import urllib.request
import util.log
import util.net
import util.time
from pool.thread import submit

def _http_post(url, data='', timeout=s4.max_timeout):
    try:
        with urllib.request.urlopen(url, data.encode(), timeout=timeout) as resp:
            body = resp.read().decode()
            code = resp.status
    except urllib.error.HTTPError as e:
        if e.code == 429:
            logging.info(e.msg)
            sys.exit(42)
        else:
            return {'body': e.msg + e.fp.read().decode(), 'code': e.code}
    else:
        return {'body': body, 'code': code}

def _http_get(url, timeout=s4.max_timeout):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode()
            code = resp.status
    except urllib.error.HTTPError as e:
        return {'body': e.msg + e.fp.read().decode(), 'code': e.code}
    else:
        return {'body': body, 'code': code}

def rm(prefix, recursive=False):
    """
    delete data from s4.

    - recursive to delete directories.
    """
    assert prefix.startswith('s4://')
    _rm(prefix, recursive)

def _rm(prefix, recursive):
    if recursive:
        fs = [pool.thread.submit(_http_post, f'http://{address}:{port}/delete?prefix={prefix}&recursive=true')
              for address, port in s4.servers()]
        for f in concurrent.futures.as_completed(fs):
            resp = f.result()
            assert resp['code'] == 200, resp
    else:
        server = s4.pick_server(prefix)
        resp = _http_post(f'http://{server}/delete?prefix={prefix}')
        assert resp['code'] == 200, resp

def eval(key, cmd):
    """
    eval a bash cmd with key data as stdin
    """
    resp = _http_post(f'http://{s4.pick_server(key)}/eval?key={key}', cmd)
    if resp['code'] == 404:
        logging.info('fatal: no such key')
        sys.exit(1)
    elif resp['code'] == 400:
        result = json.loads(resp['body'])
        logging.info(result['stdout'])
        logging.info(result['stderr'])
        logging.info(f'exitcode={result["exitcode"]}')
        sys.exit(1)
    else:
        assert resp['code'] == 200, resp
        print(resp['body'])

@argh.arg('prefix', nargs='?', default=None)
def ls(prefix, recursive=False):
    """
    list keys
    """
    if prefix and '://' not in prefix:
        prefix = f's4://{prefix}'
    lines = []
    if prefix:
        if not recursive and prefix.split('://', 1)[-1].count('/') == 0:
            for line in _ls_buckets():
                if prefix.split('://', 1)[-1] in line:
                    lines = [line]
                    break
        else:
            lines = _ls(prefix, recursive)
    else:
        lines = _ls_buckets()
    assert lines
    just = max(len(size) for date, time, size, path in lines)
    for date, time, size, path in lines:
        print(date.ljust(10), time.ljust(8), size.rjust(just), path)

def _ls(prefix, recursive):
    recursive = '&recursive=true' if recursive else ''
    fs = [submit(_http_get, f'http://{address}:{port}/list?prefix={prefix}{recursive}') for address, port in s4.servers()]
    for f in fs:
        assert f.result()['code'] == 200, f.result()
    res = [json.loads(f.result()['body']) for f in fs]
    return sorted(set(tuple(line) for lines in res for line in lines), key=lambda x: x[-1])

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
    bucket, *parts = src.split('s4://', 1)[-1].rstrip('/').split('/')
    prefix = '/'.join(parts) or bucket + '/'
    for line in _ls(src, recursive=True):
        date, time, size, key = line
        token = os.path.dirname(prefix) if dst == '.' else prefix
        path = os.path.join(dst, key.split(token or None, 1)[-1].lstrip(' /'))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cp('s4://' + os.path.join(bucket, key), path)

def _put_recursive(src, dst):
    for dirpath, dirs, files in os.walk(src):
        path = dirpath.split(src, 1)[-1].lstrip('/')
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
            logging.info('fatal: no such key')
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
    assert resp['code'] != 409, f'fatal: key already exists: {dst}'
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
    """
    copy data to or from s4.

    - paths can be:
      - remote:       "s4://bucket/key.txt"
      - local:        "./dir/key.txt"
      - stdin/stdout: "-"
    - use recursive to copy directories.
    - keys cannot be updated, but can be deleted then recreated.
    - note: to copy from s4, the local machine must be reachable by the servers, otherwise use `s4 eval`.
    - server placement is based on either the path hash or a numeric prefix:
      - hash full key path: s4://bucket/dir/name.txt
      - use numeric prefix: s4://bucket/dir/000_bucket0.txt
      - use numeric prefix: s4://bucket/dir/000
    """
    assert not (src.startswith('s4://') and dst.startswith('s4://')), 'fatal: there is no move, there is only cp and rm.'
    assert ' ' not in src and ' ' not in dst, 'fatal: spaces in keys are not allowed'
    assert not dst.startswith('s4://') or not dst.split('s4://', 1)[-1].startswith('_'), 'fatal: buckets cannot start with underscore'
    assert not src.startswith('s4://') or not src.split('s4://', 1)[-1].startswith('_'), 'fatal: buckets cannot start with underscore'
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

def _post_all(urls):
    fs = {submit(_http_post, url, data): (url, data) for url, data in urls}
    for f in concurrent.futures.as_completed(list(fs)):
        url, data = fs.pop(f)
        resp = f.result()
        if resp['code'] == 400:
            result = json.loads(resp['body'])
            logging.info(f'fatal: cmd failure {url}')
            logging.info(result['stdout'])
            logging.info(result['stderr'])
            logging.info(f'exitcode={result["exitcode"]}')
            sys.exit(1)
        elif resp['code'] == 409:
            logging.info(f'fatal: {url}')
            logging.info(resp['body'])
            sys.exit(1)
        else:
            assert resp['code'] == 200, f'{resp["code"]} {url}\n{resp["body"]}'
            print('ok', end=' ', file=sys.stderr, flush=True)
    print('', file=sys.stderr, flush=True)

def _parse_glob(indir):
    if '*' in indir:
        base = []
        pattern = []
        switch = False
        for part in indir.split('/'):
            if '*' in part:
                switch = True
            if switch:
                pattern.append(part)
            else:
                base.append(part)
        indir = '/'.join(base) + '/'
        glob = '/'.join(pattern)
    else:
        glob = None
    return indir, glob

def map(indir, outdir, cmd):
    """
    process data.

    - map a bash cmd 1:1 over every key in indir putting result in outdir.
    - cmd receives data via stdin and returns data via stdout.
    - every key in indir will create a key with the same name in outdir.
    - indir will be listed recursively to find keys to map.
    - only keys with a numeric prefix can be mapped since outputs need to be on the same server.
    - server placement is based on either path hash or a numeric prefix:
      - hash full key path: s4://bucket/dir/name.txt
      - use numeric prefix: s4://bucket/dir/000_bucket0.txt
      - use numeric prefix: s4://bucket/dir/000
    """
    indir, glob = _parse_glob(indir)
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    lines = _ls(indir, recursive=True)
    proto, path = indir.split('://')
    bucket, path = path.split('/', 1)
    datas = collections.defaultdict(list)
    for line in lines:
        date, time, size, key = line
        key = key.split(path or None, 1)[-1]
        if size == 'PRE':
            continue
        if glob and not fnmatch.fnmatch(key, glob):
            continue
        assert s4.key_bucket_num(key).isdigit(), f'keys must be prefixed with digits to be colocated, see: s4.pick_server(key). got: {s4.key_bucket_num(key)}'
        inkey = os.path.join(indir, key)
        outkey = os.path.join(outdir, key)
        assert s4.pick_server(inkey) == s4.pick_server(outkey)
        datas[s4.pick_server(inkey)].append([inkey, outkey])
    urls = [(f'http://{server}/map', json.dumps({'cmd': cmd, 'args': data})) for server, data in datas.items()]
    _post_all(urls)

def map_to_n(indir, outdir, cmd):
    """
    shuffle data.

    - map a bash cmd 1:n over every key in indir putting results in outdir.
    - cmd receives data via stdin, writes files to disk, and returns file paths via stdout.
    - every key in indir will create a directory with the same name in outdir.
    - outdir directories contain zero or more files output by cmd.
    - cmd runs in a tempdir which is deleted on completion.
    - input like: .../indir/000_a_key
    - output like: .../outdir/000_a_key/000_bucket0_from_a_key
    - server placement is based on either the path hash or a numeric prefix:
      - hash full key path: s4://bucket/dir/name.txt
      - use numeric prefix: s4://bucket/dir/000_bucket0.txt
      - use numeric prefix: s4://bucket/dir/000
    """
    indir, glob = _parse_glob(indir)
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    lines = _ls(indir, recursive=True)
    proto, path = indir.split('://')
    bucket, path = path.split('/', 1)
    urls = []
    datas = collections.defaultdict(list)
    for line in lines:
        date, time, size, key = line
        key = key.split(path or None, 1)[-1]
        assert size != 'PRE', key
        assert s4.key_bucket_num(key).isdigit(), f'keys must be prefixed with digits to be colocated, see: s4.pick_server(key). got: {s4.key_bucket_num(key)}'
        if glob and not fnmatch.fnmatch(key, glob):
            continue
        inkey = os.path.join(indir, key)
        datas[s4.pick_server(inkey)].append((inkey, outdir))
    urls = [(f'http://{server}/map_to_n', json.dumps({'cmd': cmd, 'args': data})) for server, data in datas.items()]
    _post_all(urls)

def map_from_n(indir, outdir, cmd):
    """
    merge shuffled data.

    - map a bash cmd n:1 over every key in indir putting result in outdir.
    - indir will be listed recursively to find keys to map.
    - cmd receives file paths via stdin and returns data via stdout.
    - each cmd receives all keys for a numeric prefix.
    - output name is the numeric prefix plus any suffix.
    - input like: .../indir/000_a_key/000_bucket0_from_a_key
    - output like: .../outdir/000_bucket0_from_all_keys
    """
    indir, glob = _parse_glob(indir)
    assert indir.endswith('/'), 'indir must be a directory'
    assert outdir.endswith('/'), 'outdir must be a directory'
    lines = _ls(indir, recursive=True)
    buckets = collections.defaultdict(list)
    bucket, indir = indir.split('://', 1)[-1].split('/', 1)
    for line in lines:
        date, time, size, key = line
        key = key.split(indir or None, 1)[-1]
        bucket_num = s4.key_bucket_num(key)
        assert bucket_num.isdigit(), f'keys must be prefixed with digits to be colocated, see: s4.pick_server(dir). got: {bucket_num}'
        if glob and not fnmatch.fnmatch(key, glob):
            continue
        buckets[bucket_num].append(os.path.join(f's4://{bucket}', indir, key))
    datas = collections.defaultdict(list)
    for bucket_num, inkeys in buckets.items():
        servers = [s4.pick_server(k) for k in inkeys]
        assert len(set(servers)) == 1, set(servers)
        datas[servers[0]].append(inkeys)
    urls = [(f'http://{server}/map_from_n?outdir={outdir}', json.dumps({'cmd': cmd, 'args': inkeys})) for server, inkeys in datas.items()]
    _post_all(urls)

def config():
    """
    list the server addresses
    """
    return [':'.join(x) for x in s4.servers()]

def health():
    """
    health check every server
    """
    fail = False
    fs = {}
    for addr, port in s4.servers():
        f = submit(_http_get, f'http://{addr}:{port}/health', timeout=1)
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
    shell.ignore_closed_pipes()
    util.log.setup(format='%(message)s')
    pool.thread.size = len(s4.servers())
    try:
        shell.dispatch_commands(globals(), __name__)
    except AssertionError as e:
        if e.args:
            logging.info(util.colors.red(e.args[0]))
        sys.exit(1)
