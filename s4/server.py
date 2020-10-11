#!/usr/bin/env pypy3
import traceback
import argh
import asyncio
import concurrent.futures
import datetime
import json
import logging
import os
import s4
import s4.cli
import shutil
import stat
import sys
import tempfile
import time
import tornado.concurrent
import tornado.gen
import tornado.ioloop
import tornado.util
import util.log
import util.misc
import uuid
import web
import util.retry

io_jobs = {}

# setup pools
io_send_pool = None # concurrent.futures.ThreadPoolExecutor(max_io_jobs)
io_recv_pool = None # concurrent.futures.ThreadPoolExecutor(max_io_jobs)
cpu_pool     = None # concurrent.futures.ThreadPoolExecutor(max_cpu_jobs)
misc_pool    = None # concurrent.futures.ThreadPoolExecutor(max_cpu_jobs)
solo_pool    = None # concurrent.futures.ThreadPoolExecutor(1)

# pool submit fns
submit_io_send = lambda f, *a, **kw: tornado.ioloop.IOLoop.current().run_in_executor(io_send_pool, lambda: f(*a, **kw)) # type: ignore # noqa
submit_io_recv = lambda f, *a, **kw: tornado.ioloop.IOLoop.current().run_in_executor(io_recv_pool, lambda: f(*a, **kw)) # type: ignore # noqa
submit_cpu     = lambda f, *a, **kw: tornado.ioloop.IOLoop.current().run_in_executor(cpu_pool,     lambda: f(*a, **kw)) # type: ignore # noqa
submit_misc    = lambda f, *a, **kw: tornado.ioloop.IOLoop.current().run_in_executor(misc_pool,    lambda: f(*a, **kw)) # type: ignore # noqa
submit_solo    = lambda f, *a, **kw: tornado.ioloop.IOLoop.current().run_in_executor(solo_pool,    lambda: f(*a, **kw)) # type: ignore # noqa

printf = "-printf '%TY-%Tm-%Td %TH:%TM:%TS %s %p\n'"
perm = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in io_jobs:
            io_jobs[val] = {'start': time.monotonic()}
            return val
    assert False

def checksum_write(path, checksum):
    with open(checksum_path(path), 'w') as f:
        f.write(checksum)

def checksum_read(path):
    with open(checksum_path(path)) as f:
        return f.read()

def checksum_path(path):
    assert not path.endswith('/')
    return f'{path}.xxh3'

def exists(path):
    return os.path.isfile(path) and os.path.isfile(checksum_path(path))

def confirm_local_put(temp_path, path, checksum):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    assert not os.path.isfile(path), f'fatal: key already exists: s4://{path}'
    assert not os.path.isfile(checksum_path(path))
    os.rename(temp_path, path)
    checksum_write(path, checksum)
    os.chmod(checksum_path(path), perm)
    os.chmod(path, perm)

def xxh3_checksum(path):
    result = s4.run(f'< {path} xxh3')
    assert result['exitcode'] == 0, result
    return result['stdout']

async def local_put(temp_path, key):
    assert ' ' not in key
    assert s4.on_this_server(key)
    path = key.split('s4://', 1)[-1]
    assert not path.startswith('_')
    checksum = await submit_misc(xxh3_checksum, temp_path)
    await submit_solo(confirm_local_put, temp_path, path, checksum)

def prepare_put(path):
    port = util.net.free_port()
    assert not os.path.isfile(path)
    assert not os.path.isfile(checksum_path(path))
    return s4.new_temp_path('_tempfiles'), port

def start(func, timeout):
    future = tornado.concurrent.Future()
    add_callback = tornado.ioloop.IOLoop.current().add_callback
    def fn(*a, **kw):
        add_callback(future.set_result, None)
        return func(*a, **kw)
    future = tornado.gen.with_timeout(datetime.timedelta(seconds=timeout), future)
    return future, fn

@s4.return_stacktrace
async def prepare_put_handler(request: web.Request) -> web.Response:
    [key] = request['query']['key']
    assert ' ' not in key
    assert s4.on_this_server(key)
    path = key.split('s4://', 1)[-1]
    assert not path.startswith('_')
    try:
        temp_path, port = await submit_solo(prepare_put, path)
    except AssertionError:
        return {'code': 409, 'body': traceback.format_exc()}
    else:
        try:
            started, s4_run = start(s4.run, s4.timeout)
            uuid = new_uuid()
            assert not os.path.isfile(temp_path)
            io_jobs[uuid] = {'start': time.monotonic(),
                             'future': submit_io_recv(s4_run, f'recv {port} | xxh3 --stream > {temp_path}'),
                             'temp_path': temp_path,
                             'path': path}
            try:
                await started
            except tornado.util.TimeoutError:
                job = io_jobs.pop(uuid)
                job['future'].cancel()
                await submit_solo(s4.delete, checksum_path(path), temp_path)
                return {'code': 429, 'body': 'server busy timeout, please retry'}
            else:
                return {'code': 200, 'body': json.dumps([uuid, port])}
        except:
            s4.delete(checksum_path(path))
            raise

def confirm_put(path, temp_path, server_checksum):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        assert not os.path.isfile(path)
        assert not os.path.isfile(checksum_path(path))
        checksum_write(path, server_checksum)
        os.chmod(temp_path, perm)
        os.rename(temp_path, path)
        os.chmod(checksum_path(path), perm)
    except:
        s4.delete(path, temp_path, checksum_path(path))
        raise

@s4.return_stacktrace
async def confirm_put_handler(request: web.Request) -> web.Response:
    [uuid] = request['query']['uuid']
    [client_checksum] = request['query']['checksum']
    job = io_jobs.pop(uuid)
    result = await job['future']
    assert result['exitcode'] == 0, result
    server_checksum = result['stderr']
    assert client_checksum == server_checksum, [client_checksum, server_checksum, result]
    await submit_solo(confirm_put, job['path'], job['temp_path'], server_checksum)
    return {'code': 200}

@s4.return_stacktrace
async def eval_handler(request: web.Request) -> web.Response:
    [key] = request['query']['key']
    cmd = request['body'].strip()
    assert s4.on_this_server(key)
    path = key.split('s4://', 1)[-1]
    if not await submit_solo(exists, path):
        return {'code': 404}
    else:
        result = await submit_cpu(s4.run, f'< {path} {cmd}', timeout=s4.timeout) # type: ignore
        if result['exitcode'] == 0:
            return {'code': 200, 'body': result['stdout']}
        else:
            return {'code': 400, 'body': json.dumps(result)}

@s4.return_stacktrace
async def prepare_get_handler(request: web.Request) -> web.Response:
    [key] = request['query']['key']
    [port] = request['query']['port']
    remote = request['remote']
    assert s4.on_this_server(key)
    path = key.split('s4://', 1)[-1]
    if not await submit_solo(exists, path):
        return {'code': 404}
    else:
        started, s4_run = start(s4.run, s4.timeout)
        uuid = new_uuid()
        io_jobs[uuid] = {'start': time.monotonic(),
                         'future': submit_io_send(s4_run, f'< {path} xxh3 --stream | send {remote} {port}'),
                         'disk_checksum': await submit_solo(checksum_read, path)}
        try:
            await started
        except tornado.util.TimeoutError:
            job = io_jobs.pop(uuid)
            job['future'].cancel()
            return {'code': 429, 'body': 'server busy timeout, please retry'}
        else:
            return {'code': 200, 'body': uuid}

@s4.return_stacktrace
async def confirm_get_handler(request: web.Request) -> web.Response:
    [uuid] = request['query']['uuid']
    [client_checksum] = request['query']['checksum']
    job = io_jobs.pop(uuid)
    result = await job['future']
    assert result['exitcode'] == 0, result
    server_checksum = result['stderr']
    assert job['disk_checksum'] == client_checksum == server_checksum, [job['disk_checksum'], client_checksum, server_checksum]
    return {'code': 200}

@s4.return_stacktrace
async def list_buckets_handler(request: web.Request) -> web.Response:
    result = await submit_misc(s4.run, f'find -maxdepth 1 -mindepth 1 -type d ! -name "_*" {printf}')
    assert result['exitcode'] == 0, result
    xs = [x.split() for x in result['stdout'].splitlines()]
    xs = [[date, time.split('.')[0], size, os.path.basename(path)] for date, time, size, path in xs]
    return {'code': 200, 'body': json.dumps(xs)}

@s4.return_stacktrace
async def list_handler(request: web.Request) -> web.Response:
    [prefix] = request['query']['prefix']
    assert prefix.startswith('s4://')
    token = prefix = prefix.split('s4://', 1)[-1]
    if not token.endswith('/'):
        token = os.path.dirname(token) + '/'
    recursive = request['query'].get('recursive', [''])[0] == 'true'
    if recursive:
        if not prefix.endswith('/'):
            prefix += '*'
        result = await submit_misc(s4.run, f"find {prefix} -type f ! -name '*.xxh3' {printf}")
        assert result['exitcode'] == 0 or 'No such file or directory' in result['stderr'], result
        xs = [x.split() for x in result['stdout'].splitlines()]
        xs = [[date, time.split('.')[0], size, '/'.join(path.split('/')[1:])] for date, time, size, path in xs]
    else:
        name = ''
        if not prefix.endswith('/'):
            name = os.path.basename(prefix)
            name = f"-name '{name}*'"
            prefix = os.path.dirname(prefix)
        result = await submit_misc(s4.run, f"find {prefix} -maxdepth 1 -type f ! -name '*.xxh3' {name} {printf}")
        assert result['exitcode'] == 0 or 'No such file or directory' in result['stderr'], result
        files = result['stdout']
        result = await submit_misc(s4.run, f"find {prefix} -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' {name}")
        assert result['exitcode'] == 0 or 'No such file or directory' in result['stderr'], result
        files = [x.split() for x in files.splitlines() if x.split()[-1].strip()]
        dirs = [('', '', 'PRE', x + '/') for x in result['stdout'].splitlines()]
        xs = [[date, time.split(".")[0], size, path.split(token, 1)[-1]] for date, time, size, path in files + dirs]
        xs = [[date, time, size, path] for date, time, size, path in xs if path.strip()]
    return {'code': 200, 'body': json.dumps(xs)}

@s4.return_stacktrace
async def delete_handler(request: web.Request) -> web.Response:
    [prefix] = request['query']['prefix']
    assert prefix.startswith('s4://')
    prefix = prefix.split('s4://', 1)[-1]
    assert not prefix.startswith('/'), prefix
    recursive = request['query'].get('recursive', [''])[0] == 'true'
    if recursive:
        result = await submit_solo(s4.run, 'rm -rf', prefix + '*')
    else:
        result = await submit_solo(s4.run, 'rm -f', prefix, checksum_path(prefix))
    assert result['exitcode'] == 0, result
    return {'code': 200}

async def health_handler(request: web.Request) -> web.Response:
    return {'code': 200}

def create_task(fn):
    return asyncio.get_event_loop().create_task(fn)

@s4.return_stacktrace
async def map_handler(request: web.Request) -> web.Response:
    data = json.loads(request['body'])
    cmd = data['cmd'].strip()
    if cmd.startswith('while read'):
        cmd = f'cat | {cmd}'
    fs = []
    for inkey, outkey in data['args']:
        assert s4.on_this_server(inkey)
        assert s4.on_this_server(outkey)
        inpath = os.path.abspath(inkey.split('s4://', 1)[-1])
        run = lambda inpath, outkey, cmd: [outkey, run_in_tempdir(f'< {inpath} {cmd} > output', env={'filename': os.path.basename(inpath)})]
        fs.append(submit_cpu(run, inpath, outkey, cmd))
    tempdirs = []
    try:
        put_fs = []
        for f in asyncio.as_completed(fs, timeout=s4.max_timeout):
            outkey, (tempdir, result) = await f
            tempdirs.append(tempdir)
            if result['exitcode'] != 0:
                for f in fs: # type: ignore
                    f.cancel()
                return {'code': 400, 'body': json.dumps(result)}
            else:
                temp_path = os.path.join(tempdir, 'output')
                put_fs.append(create_task(local_put(temp_path, outkey)))
        await asyncio.gather(*put_fs)
    except AssertionError:
        return {'code': 409, 'body': traceback.format_exc()}
    except asyncio.TimeoutError:
        return {'code': 429, 'body': json.dumps({'stderr': 'server busy timeout, please retry', 'stdout': '', 'exitcode': 1})}
    else:
        return {'code': 200}
    finally:
        await submit_misc(s4.delete_dirs, tempdirs)

def retry_except_404_and_409(exception):
    if exception.args:
        val = exception.args[0]
        if isinstance(val, dict) and 'No such file or directory' in val.get('stderr', ''): # allow 404
            return True
        elif isinstance(val, str) and val.startswith('fatal: key already exists: '): # allow 409
            return True
    return False
retry_put = lambda f: util.retry.retry(f, allowed_exception_fn=retry_except_404_and_409, times=1000, exponent=1.2, max_seconds=120, stacktrace=False)

@s4.return_stacktrace
async def map_to_n_handler(request: web.Request) -> web.Response:
    data = json.loads(request['body'])
    cmd = data['cmd'].strip()
    if cmd.startswith('while read'):
        cmd = f'cat | {cmd}'
    fs = []
    for inkey, outdir in data['args']:
        assert s4.on_this_server(inkey)
        assert outdir.startswith('s4://') and outdir.endswith('/')
        inpath = os.path.abspath(inkey.split('s4://', 1)[-1])
        run = lambda inpath, outdir, cmd: [inpath, outdir, run_in_tempdir(f'< {inpath} {cmd}', env={'filename': os.path.basename(inpath)})]
        fs.append(submit_cpu(run, inpath, outdir, cmd))
    tempdirs = []
    try:
        put_fs = []
        for f in asyncio.as_completed(fs, timeout=s4.max_timeout):
            inpath, outdir, (tempdir, result) = await f
            tempdirs.append(tempdir)
            if result['exitcode'] != 0:
                for f in fs: # type: ignore
                    f.cancel()
                return {'code': 400, 'body': json.dumps(result)}
            else:
                for temp_path in result['stdout'].splitlines():
                    temp_path = os.path.join(tempdir, temp_path)
                    outkey = os.path.join(outdir, os.path.basename(inpath), os.path.basename(temp_path))
                    put_fs.append(map_to_n_put(temp_path, outkey))
        await asyncio.gather(*put_fs)
    except AssertionError:
        return {'code': 409, 'body': traceback.format_exc()}
    except asyncio.TimeoutError:
        return {'code': 429, 'body': json.dumps({'stderr': 'server busy timeout, please retry', 'stdout': '', 'exitcode': 1})}
    else:
        return {'code': 200}
    finally:
        await submit_misc(s4.delete_dirs, tempdirs)

def map_to_n_put(temp_path, outkey):
    if s4.on_this_server(outkey):
        return create_task(local_put(temp_path, outkey))
    else:
        return submit_io_send(retry_put(s4.cli._put), temp_path, outkey)

@s4.return_stacktrace
async def map_from_n_handler(request: web.Request) -> web.Response:
    [outdir] = request['query']['outdir']
    assert outdir.startswith('s4://') and outdir.endswith('/')
    data = json.loads(request['body'])
    cmd = data['cmd'].strip()
    if cmd.startswith('while read'):
        cmd = f'cat | {cmd}'
    fs = []
    for inkeys in data['args']:
        assert all(s4.on_this_server(key) for key in inkeys)
        outkey = os.path.join(outdir, s4.key_prefix(inkeys[0]) + s4.suffix(inkeys))
        inpaths = [os.path.abspath(inkey.split('s4://', 1)[-1]) for inkey in inkeys]
        run = lambda inpaths, outkey, cmd: [outkey, run_in_tempdir(f'{cmd} > output', stdin='\n'.join(inpaths) + '\n')]
        fs.append(submit_cpu(run, inpaths, outkey, cmd))
    tempdirs = []
    try:
        put_fs = []
        for f in asyncio.as_completed(fs, timeout=s4.max_timeout):
            outkey, (tempdir, result) = await f
            tempdirs.append(tempdir)
            if result['exitcode'] != 0:
                for f in fs: # type: ignore
                    f.cancel()
                return {'code': 400, 'body': json.dumps(result)}
            else:
                temp_path = os.path.join(tempdir, 'output')
                put_fs.append(create_task(local_put(temp_path, outkey)))
        await asyncio.gather(*put_fs)
    except AssertionError:
        return {'code': 409, 'body': traceback.format_exc()}
    except asyncio.TimeoutError:
        return {'code': 429, 'body': json.dumps({'stderr': 'server busy timeout, please retry', 'stdout': '', 'exitcode': 1})}
    else:
        return {'code': 200}
    finally:
        await submit_misc(s4.delete_dirs, tempdirs)

def run_in_tempdir(*a, **kw):
    tempdir = tempfile.mkdtemp(dir='_tempdirs')
    return tempdir, s4.run(*a, **kw, cwd=tempdir)

@util.misc.exceptions_kill_pid
async def gc_expired_data():
    for uid, job in list(io_jobs.items()):
        if job and time.monotonic() - job['start'] > s4.max_timeout:
            logging.info(f'gc expired job: {job}')
            with util.exceptions.ignore(KeyError):
                await submit_misc(s4.delete, checksum_path(job['path']), job['temp_path'])
            io_jobs.pop(uid, None)
    result = await submit_misc(s4.run, f'find _tempfiles/ -mindepth 1 -maxdepth 1 -type f -cmin +{int(s4.max_timeout / 60) + 1}')
    if result['exitcode'] == 0:
        tempfiles = result['stdout'].splitlines()
        for path in tempfiles:
            logging.info(f'gc expired tempfile: {path}')
            await submit_misc(s4.delete, path)
    result = await submit_misc(s4.run, f'find _tempdirs/  -mindepth 1 -maxdepth 1 -type d -cmin +{int(s4.max_timeout / 60) + 1}')
    if result['exitcode'] == 0:
        tempdirs = result['stdout'].splitlines()
        for tempdir in tempdirs:
            logging.info(f'gc expired tempdir: {tempdir}')
            await submit_misc(shutil.rmtree, tempdir)
    await tornado.gen.sleep(5)
    tornado.ioloop.IOLoop.current().add_callback(gc_expired_data)

def init_pools(max_io_jobs, max_cpu_jobs):
    globals()['io_send_pool'] = concurrent.futures.ThreadPoolExecutor(max_io_jobs)
    globals()['io_recv_pool'] = concurrent.futures.ThreadPoolExecutor(max_io_jobs)
    globals()['cpu_pool']     = concurrent.futures.ThreadPoolExecutor(max_cpu_jobs)
    globals()['misc_pool']    = concurrent.futures.ThreadPoolExecutor(max_cpu_jobs)
    globals()['solo_pool']    = concurrent.futures.ThreadPoolExecutor(1)

num_cpus     = os.cpu_count() or 1
max_io_jobs  = num_cpus * 4
max_cpu_jobs = num_cpus + 2

def main(debug=False, port=None, max_io_jobs=max_io_jobs, max_cpu_jobs=max_cpu_jobs):
    util.log.setup(format='%(message)s')
    if port:
        s4.http_port = lambda: port
    init_pools(max_io_jobs, max_cpu_jobs)
    if not os.path.basename(os.getcwd()) == 's4_data':
        os.makedirs('s4_data/_tempfiles', exist_ok=True)
        os.makedirs('s4_data/_tempdirs',  exist_ok=True)
        os.chdir('s4_data')
    os.environ['LC_ALL'] = 'C'
    routes = [('/prepare_put',  {'post': prepare_put_handler}),
              ('/confirm_put',  {'post': confirm_put_handler}),
              ('/prepare_get',  {'post': prepare_get_handler}),
              ('/confirm_get',  {'post': confirm_get_handler}),
              ('/delete',       {'post': delete_handler}),
              ('/map',          {'post': map_handler}),
              ('/map_to_n',     {'post': map_to_n_handler}),
              ('/map_from_n',   {'post': map_from_n_handler}),
              ('/eval',         {'post': eval_handler}),
              ('/list',         {'get':  list_handler}),
              ('/list_buckets', {'get':  list_buckets_handler}),
              ('/health',       {'get':  health_handler})]
    port = s4.http_port()
    logging.info(f'starting s4 server on port: {port}')
    web.app(routes, debug=debug).listen(port, idle_connection_timeout=s4.max_timeout, body_timeout=s4.max_timeout)
    tornado.ioloop.IOLoop.current().add_callback(gc_expired_data)
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == '__main__':
    argh.dispatch_command(main)
