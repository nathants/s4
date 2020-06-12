#!/usr/bin/env pypy3
import argh
import concurrent.futures
import json
import logging
import os
import s4
import sys
import tempfile
import time
import tornado.gen
import tornado.ioloop
import util.log
import uuid
import web

jobs = {}
path_prefix = '_s4_data'
max_jobs = int(os.environ.get('S4_MAX_JOBS', os.cpu_count() * 8)) # type: ignore
nc_pool = concurrent.futures.ThreadPoolExecutor(max_jobs)
default_pool = concurrent.futures.ThreadPoolExecutor(max_jobs)

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in jobs:
            jobs[val] = None
            return val
    assert False

def checksum_write(path, checksum):
    with open(xxh3_path(path), 'w') as f:
        f.write(checksum)

def checksum_read(path):
    with open(xxh3_path(path)) as f:
        return f.read()

def exists(path):
    return os.path.isfile(path) and os.path.isfile(xxh3_path(path))

def xxh3_path(key):
    parts = key.split('/')
    parts = parts[:-1] + [f'.{parts[-1]}.xxh3']
    return '/'.join(parts)

def prepare_put(path):
    parent = os.path.dirname(path)
    _, temp_path = tempfile.mkstemp(dir='.')
    port = util.net.free_port()
    os.makedirs(parent, exist_ok=True)
    return temp_path, port

async def prepare_put_handler(req):
    if len(jobs) > max_jobs:
        return {'code': 429}
    else:
        key = req['query']['key']
        assert s4.on_this_server(key)
        path = os.path.join(path_prefix, key.split('s4://')[-1])
        temp_path, port = await submit(prepare_put, path)
        uuid = new_uuid()
        jobs[uuid] = {'time': time.monotonic(),
                      'future': submit(s4.run, f'recv {port} | xxh3 --stream > {temp_path}', executor=nc_pool),
                      'temp_path': temp_path,
                      'path': path}
        return {'code': 200, 'body': json.dumps([uuid, port])}

async def confirm_put_handler(req):
    uuid = req['query']['uuid']
    client_checksum = req['query']['checksum']
    job = jobs.pop(uuid)
    result = await job['future']
    assert result['exitcode'] == 0, result
    server_checksum = result['stderr']
    assert client_checksum == server_checksum, [client_checksum, server_checksum, result]
    await submit(checksum_write, job['path'], server_checksum)
    await submit(os.rename, job['temp_path'], job['path'])
    return {'code': 200}

async def prepare_get_handler(req):
    key = req['query']['key']
    port = req['query']['port']
    remote = req['remote']
    assert s4.on_this_server(key)
    path = os.path.join(path_prefix, key.split('s4://')[-1])
    if not (await submit(exists, path)):
        return {'code': 404}
    else:
        uuid = new_uuid()
        jobs[uuid] = {'time': time.monotonic(),
                      'future': submit(s4.run, f'xxh3 --stream < {path} | send {remote} {port}', executor=nc_pool),
                      'path': path}
        return {'code': 200, 'body': uuid}

async def confirm_get_handler(req):
    uuid = req['query']['uuid']
    client_checksum = req['query']['checksum']
    job = jobs.pop(uuid)
    result = await job['future']
    assert result['exitcode'] == 0, result
    server_checksum = result['stderr']
    try:
        disk_checksum = await submit(checksum_read, job['path'])
    except FileNotFoundError:
        return {'code': 404}
    else:
        assert disk_checksum == server_checksum, [disk_checksum, server_checksum]
        assert client_checksum == server_checksum, [client_checksum, server_checksum]
        return {'code': 200}

async def list_handler(req):
    _prefix = req['query']['prefix']
    assert _prefix.startswith('s4://'), _prefix
    _prefix = _prefix.split('s4://')[-1]
    prefix = os.path.join(path_prefix, _prefix)
    recursive = req['query'].get('recursive') == 'true'
    if recursive:
        if not prefix.endswith('/'):
            prefix += '*'
        res = await submit(s4.run, f"find {prefix} -type f ! -name '*.xxh3'")
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        xs = res['stdout'].splitlines()
        xs = ['/'.join(x.split('/')[2:]) for x in xs]
    else:
        name = ''
        if not prefix.endswith('/'):
            name = os.path.basename(prefix)
            name = f"-name '{name}*'"
            prefix = os.path.dirname(prefix)
        res = await submit(s4.run, f"find {prefix} -maxdepth 1 -type f ! -name '*.xxh3' {name}")
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        files = res['stdout']
        res = await submit(s4.run, f"find {prefix} -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' {name}")
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        dirs = res['stdout']
        xs = files.splitlines() + [x + '/' for x in dirs.splitlines()]
        if not _prefix.endswith('/'):
            _prefix = os.path.dirname(_prefix) + '/'
        xs = [x.split(_prefix)[-1] for x in xs]
    return {'code': 200, 'body': json.dumps(xs)}

async def delete_handler(req):
    prefix = req['query']['prefix']
    assert prefix.startswith('s4://'), prefix
    prefix = prefix.split('s4://')[-1]
    recursive = req['query'].get('recursive') == 'true'
    prefix = os.path.join(path_prefix, prefix)
    if recursive:
        resp = await submit(s4.run, 'rm -rf', prefix + '*')
    else:
        resp = await submit(s4.run, 'rm -f', prefix, xxh3_path(prefix))
    assert resp['exitcode'] == 0
    return {'code': 200}

async def health(req):
    return {'code': 200}

def submit(f, *a, executor=None, **kw):
    return tornado.ioloop.IOLoop.current().run_in_executor(executor or default_pool, lambda: f(*a, **kw))

async def gc_jobs():
    try:
        for job in list(jobs):
            with util.exceptions.ignore(FileNotFoundError, KeyError):
                if time.monotonic() - jobs[job]['time'] > s4.timeout:
                    await submit(os.remove, jobs.pop(job)['temp_path'])
    except:
        logging.exception('proc garbage collector died')
        time.sleep(1)
        os._exit(1)
    else:
        await tornado.gen.sleep(5)
        tornado.ioloop.IOLoop.current().add_callback(gc_jobs)

def start(debug=False):
    util.log.setup(format='%(message)s')
    routes = [('/prepare_put', {'post': prepare_put_handler}),
              ('/confirm_put', {'post': confirm_put_handler}),
              ('/prepare_get', {'post': prepare_get_handler}),
              ('/confirm_get', {'post': confirm_get_handler}),
              ('/delete',      {'post': delete_handler}),
              ('/list',        {'get':  list_handler}),
              ('/health',      {'get':  health})]
    port = s4.http_port()
    logging.info(f'starting s4 server on port: {port}')
    web.app(routes, debug=debug).listen(port)
    tornado.ioloop.IOLoop.current().add_callback(gc_jobs)
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == '__main__':
    argh.dispatch_command(start)
