import argh
import concurrent.futures
import json
import os
import pool.thread
import random
import s4
import subprocess
import sys
import time
import tornado.gen
import tornado.ioloop
import traceback
import util.log
import uuid
import web

ports_in_use = set()

jobs = {}

nc_pool = concurrent.futures.ThreadPoolExecutor(s4.max_jobs)

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in jobs:
            jobs[val] = '::taken'
            return val
    assert False

def new_port():
    for _ in range(10):
        port = random.randint(20000, 60000)
        if port not in ports_in_use:
            ports_in_use.add(port)
            return port
    assert False

def return_port(port):
    ports_in_use.remove(port)

@tornado.gen.coroutine
def prepare_put_handler(req):
    if len(jobs) > s4.max_jobs:
        return {'status': 429}
    else:
        key = req['query']['key']
        assert '0.0.0.0' == s4.pick_server(key).split(':')[0] # make sure the key is meant to live on this server before accepting it
        path = os.path.join('_s4_data', key.split('s3://')[-1])
        parent = os.path.dirname(path)
        temp_path = yield pool.thread.submit(s4.check_output, 'mktemp -p .')
        port = new_port()
        yield pool.thread.submit(s4.check_output, 'mkdir -p', parent)
        cmd = f'timeout 120 bash -c "set -euo pipefail; nc -l {port} | xxhsum > {temp_path}"'
        uuid = new_uuid()
        jobs[uuid] = {'time': time.monotonic(),
                      'future': nc_pool.submit(s4.check_output, cmd),
                      'temp_path': temp_path,
                      'path': path}
        yield pool.thread.submit(s4.check_output, f'timeout 120 bash -c "while ! netstat -ln|grep {port}; do sleep .1; done"')
        return {'status': 200, 'body': json.dumps([uuid, port])}

@tornado.gen.coroutine
def confirm_put_handler(req):
    uuid = req['query']['uuid']
    checksum = req['query']['checksum']
    job = jobs.pop(uuid)
    local_checksum = yield job['future']
    assert checksum == local_checksum, [checksum, local_checksum]
    yield pool.thread.submit(s4.check_output, 'mv', job['temp_path'], job['path'])
    return {'status': 200}

@tornado.gen.coroutine
def prepare_get_handler(req):
    yield None
    key = req['query']['key']
    port = req['query']['port']
    server = req['query']['server']
    assert '0.0.0.0' == s4.pick_server(key).split(':')[0]  # make sure the key is meant to live on this server before accepting it
    src = os.path.join('_s4_data', key.split('s3://')[-1])
    cmd = f'timeout 120 bash -c "set -euo pipefail; cat {src} | xxhsum | nc {server} {port}"'
    uuid = new_uuid()
    jobs[uuid] = {'time': time.monotonic(),
                  'future': nc_pool.submit(s4.check_output, cmd)}
    return {'status': 200, 'body': uuid}

@tornado.gen.coroutine
def confirm_get_handler(req):
    uuid = req['query']['uuid']
    checksum = req['query']['checksum']
    job = jobs.pop(uuid)
    local_checksum = yield job['future']
    assert checksum == local_checksum, [checksum, local_checksum]
    return {'status': 200}

@tornado.gen.coroutine
def list_handler(req):
    yield None
    prefix = os.path.join('_s4_data', req['query'].get('prefix', '').split('s3://')[-1])
    recursive = req['query'].get('recursive') == 'true'
    try:
        if recursive:
            val = s4.check_output(f'find {prefix}* -type f')
        else:
            if prefix.endswith('/'):
                val = s4.check_output(f'find {prefix} -maxdepth 1')
            else:
                val = s4.check_output(f"find -name '{prefix}*' -maxdepth 1")
        val = ['/'.join(x.split('/')[2:]) for x in val.splitlines()]
    except subprocess.CalledProcessError:
        val = []
    return {'status': 200,
            'body': json.dumps(val)}

@tornado.gen.coroutine
def delete_handler(req):
    yield None
    return {'status': 200}

@tornado.gen.coroutine
def return_port_handler(req):
    yield None
    return_port(int(req['body']))
    return {'status': 200}

@tornado.gen.coroutine
def new_port_handler(req):
    yield None
    if len(jobs) > s4.max_jobs:
        return {'status': 429}
    else:
        return {'status': 200, 'body': str(new_port())}

@tornado.gen.coroutine
def proc_garbage_collector():
    try:
        while True:
            for k, v in jobs.items():
                if time.monotonic() - v['time'] > 120:
                    if v.get('temp_path'):
                        s4.check_output('rm -f', v['temp_path'])
                    del jobs[k]
            yield tornado.gen.sleep(10)
    except:
        traceback.print_exc() # because if you never call result() on a coroutine, you never see its error message
        sys.stdout.flush()
        time.sleep(1)
        os._exit(1)

def start(port=s4.http_port, debug=False):
    util.log.setup()
    proc_garbage_collector()
    routes = [('/prepare_put', {'post': prepare_put_handler}),
              ('/confirm_put', {'post': confirm_put_handler}),
              ('/prepare_get', {'post': prepare_get_handler}),
              ('/confirm_get', {'post': confirm_get_handler}),
              ('/delete',      {'post': delete_handler}),
              ('/list',        {'get':  list_handler}),
              ('/new_port',    {'post': new_port_handler}),
              ('/return_port', {'post': return_port_handler})]
    web.app(routes, debug=debug).listen(port)
    tornado.ioloop.IOLoop.current().start()

def main():
    argh.dispatch_command(start)
