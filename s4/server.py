import json
import os
import random
import time
import traceback

import s4

import argh
import pool.thread
import tornado.gen
import tornado.ioloop
import util.log
import uuid
import web

ports_in_use = set()

jobs = {}

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
        assert key.startswith('s3://')
        assert not key.endswith('/')
        assert '0.0.0.0' == s4.pick_server(key) # make sure the key is meant to live on this server before accepting it
        path = key.split('s3://')[-1]
        parent = os.path.dirname(path)
        temp_path = yield pool.thread.submit(s4.check_output, 'mktemp -p .')
        port = new_port()
        yield pool.thread.submit(s4.check_output, 'mkdir -p', parent)
        cmd = f'timeout 120 bash -c "nc -q 0 -l {port} | xxhsum > {temp_path}"'
        uuid = new_uuid()
        jobs[uuid] = {'time': time.monotonic(),
                      'future': pool.thread.submit(s4.check_output, cmd),
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
    return {'status': 200}

@tornado.gen.coroutine
def confirm_get_handler(req):
    yield None
    return {'status': 200}

@tornado.gen.coroutine
def list_handler(req):
    yield None
    return {'status': 200}

@tornado.gen.coroutine
def delete_handler(req):
    yield None
    return {'status': 200}

@tornado.gen.coroutine
def proc_garbage_collector():
    try:
        while True:
            for k, v in jobs.items():
                if time.monotonic() - v['time'] > 120:
                    del jobs[k]
            yield tornado.gen.sleep(10)
    except:
        traceback.print_exc() # because if you never call result() on a coroutine, you never see its error message
        raise

def server(port=8000, debug=False):
    proc_garbage_collector()
    routes = [('/prepare_put', {'post': prepare_put_handler}),
              ('/confirm_put', {'post': confirm_put_handler}),
              ('/prepare_get', {'post': prepare_get_handler}),
              ('/confirm_get', {'post': confirm_get_handler}),
              ('/delete',      {'post': delete_handler}),
              ('/list',        {'get':  list_handler})]
    web.app(routes, debug=debug).listen(port)
    tornado.ioloop.IOLoop.current().start()

def main():
    util.log.setup()
    argh.dispatch_command(server)
