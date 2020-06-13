#!/usr/bin/env pypy3
import shell
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
max_jobs = int(os.environ.get('S4_MAX_JOBS', os.cpu_count() * 8)) # type: ignore
io_pool = concurrent.futures.ThreadPoolExecutor(max_jobs) # io is concurrent
single_pool = concurrent.futures.ThreadPoolExecutor(1) # filesystem metadata is serializable
printf = "-printf '%TY-%Tm-%Td %TH:%TM:%TS %s %p\n'"

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in jobs:
            jobs[val] = None
            return val
    assert False

def checksum_write(path, checksum):
    with open(checksum_path(path), 'w') as f:
        f.write(checksum)

def checksum_read(path):
    with open(checksum_path(path)) as f:
        return f.read()

def checksum_path(key):
    parts = key.split('/')
    parts = parts[:-1] + [f'.{parts[-1]}.xxh3']
    return '/'.join(parts)

def exists(path):
    return os.path.isfile(path) and os.path.isfile(checksum_path(path))

def prepare_put(path):
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    assert not os.path.isfile(path)
    with open(path, 'w'):
        pass # touch file to reserve the key, updates to existing keys are not allowed
    _, temp_path = tempfile.mkstemp(dir='temp')
    port = util.net.free_port()
    return temp_path, port

async def prepare_put_handler(req):
    if len(jobs) > max_jobs:
        return {'code': 429}
    else:
        key = req['query']['key']
        assert ' ' not in key
        assert s4.on_this_server(key)
        path = key.split('s4://')[-1]
        try:
            temp_path, port = await submit(prepare_put, path, executor=single_pool)
        except AssertionError:
            return {'code': 409}
        else:
            uuid = new_uuid()
            jobs[uuid] = {'time': time.monotonic(),
                          'future': submit(shell.warn, f'recv {port} | xxh3 --stream > {temp_path}', executor=io_pool),
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
    await submit(checksum_write, job['path'], server_checksum, executor=single_pool)
    await submit(os.rename, job['temp_path'], job['path'], executor=single_pool)
    return {'code': 200}

async def prepare_get_handler(req):
    key = req['query']['key']
    port = req['query']['port']
    remote = req['remote']
    assert s4.on_this_server(key)
    path = key.split('s4://')[-1]
    if await submit(exists, path, executor=single_pool):
        uuid = new_uuid()
        jobs[uuid] = {'time': time.monotonic(),
                      'future': submit(shell.warn, f'xxh3 --stream < {path} | send {remote} {port}', executor=io_pool),
                      'disk_checksum': await submit(checksum_read, path, executor=single_pool)}
        return {'code': 200, 'body': uuid}
    else:
        return {'code': 404}

async def confirm_get_handler(req):
    uuid = req['query']['uuid']
    client_checksum = req['query']['checksum']
    job = jobs.pop(uuid)
    result = await job['future']
    assert result['exitcode'] == 0, result
    server_checksum = result['stderr']
    assert job['disk_checksum'] == client_checksum == server_checksum, [job['disk_checksum'], client_checksum, server_checksum]
    return {'code': 200}

async def list_handler(req):
    prefix = req['query']['prefix']
    assert prefix.startswith('s4://')
    _prefix = prefix = prefix.split('s4://')[-1]
    recursive = req['query'].get('recursive') == 'true'
    if recursive:
        if not prefix.endswith('/'):
            prefix += '*'
        res = await submit(shell.warn, f"find {prefix} -type f ! -name '*.xxh3' {printf}", executor=single_pool)
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        xs = [x.split() for x in res['stdout'].splitlines()]
        xs = [f"{date} {time.split('.')[0]} {size} {'/'.join(path.split('/')[1:])}" for date, time, size, path in xs]
    else:
        name = ''
        if not prefix.endswith('/'):
            name = os.path.basename(prefix)
            name = f"-name '{name}*'"
            prefix = os.path.dirname(prefix)
        res = await submit(shell.warn, f"find {prefix} -maxdepth 1 -type f ! -name '*.xxh3' {name} {printf}", executor=single_pool)
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        files = res['stdout']
        res = await submit(shell.warn, f"find {prefix} -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' {name}", executor=single_pool)
        assert res['exitcode'] == 0 or 'No such file or directory' in res['stderr']
        files = [x.split() for x in files.splitlines() if x.split()[-1].strip()]
        dirs = [('_', '_', '_', x + '/') for x in res['stdout'].splitlines() if x.strip()]
        xs = files + dirs
        if not _prefix.endswith('/'):
            _prefix = os.path.dirname(_prefix) + '/'
        xs = [f'{date} {time.split(".")[0]} {size} {path.split(_prefix)[-1]}'.replace("_ _ _", "   PRE") for date, time, size, path in xs]
    return {'code': 200, 'body': json.dumps(xs)}

async def delete_handler(req):
    prefix = req['query']['prefix']
    assert prefix.startswith('s4://')
    prefix = prefix.split('s4://')[-1]
    recursive = req['query'].get('recursive') == 'true'
    if recursive:
        resp = await submit(shell.warn, 'rm -rf', prefix + '*', executor=single_pool)
    else:
        resp = await submit(shell.warn, 'rm -f', prefix, checksum_path(prefix), executor=single_pool)
    assert resp['exitcode'] == 0
    return {'code': 200}

async def health(req):
    return {'code': 200}

def submit(f, *a, executor, **kw):
    return tornado.ioloop.IOLoop.current().run_in_executor(executor, lambda: f(*a, **kw))

async def gc_jobs():
    try:
        for job in list(jobs):
            with util.exceptions.ignore(FileNotFoundError, KeyError):
                if time.monotonic() - jobs[job]['time'] > s4.timeout:
                    await submit(os.remove, jobs.pop(job)['temp_path'], executor=single_pool)
    except:
        logging.exception('proc garbage collector died')
        time.sleep(1)
        os._exit(1)
    else:
        await tornado.gen.sleep(5)
        tornado.ioloop.IOLoop.current().add_callback(gc_jobs)

def start(debug=False):
    util.log.setup(format='%(message)s')
    os.makedirs('s4_data/temp', exist_ok=True)
    os.chdir('s4_data')
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
