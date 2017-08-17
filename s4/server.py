import s4
import subprocess
import concurrent.futures
import time
import tornado.tcpclient
import s4.cli
import datetime
import json
import uuid
import argh
import os
import tornado.gen
import tornado.ioloop
import util.log
import pool.thread
import shell
import web
import random

pool.thread._size = 10

"""
recv: nc -l -p 1234 -w 3 -q 0 | ./xxHash/xxhsum > output
send: cat input | ./xxHash/xxhsum | nc 10.0.5.53 1234
"""

tcp_client = tornado.tcpclient.TCPClient()

_ports_in_use = set()

_futures = {}

pool_slow = concurrent.futures.ThreadPoolExecutor(10)
pool_fast = concurrent.futures.ThreadPoolExecutor(10)

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in _futures:
            _futures[val] = '::taken'
            return val
    assert False

def new_port():
    for _ in range(10):
        port = random.randint(20000, 60000)
        if port not in _ports_in_use:
            _ports_in_use.add(port)
            return port
    assert False

def return_port(port):
    _ports_in_use.remove(port)

@tornado.gen.coroutine
def prepare_put_handler(req):
    key = req['query']['key']
    assert key.startswith('s3://')
    assert not key.endswith('/')
    assert '0.0.0.0' == s4.cli.pick_server(key) # make sure the key is meant to live on this server
    path = key.split('s3://')[-1]
    parent = os.path.dirname(path)
    port = new_port()
    yield pool_fast.submit(shell.run, 'mkdir -p', parent)
    cmd = f'nc -q 0 -l {port} | xxhsum > {path}'
    print('cmd:', cmd)
    uuid = new_uuid()
    _futures[uuid] = subprocess.Popen(cmd, shell=True, executable='/bin/bash', stderr=subprocess.PIPE)
    return {'status': 200, 'body': json.dumps([uuid, port])}

@tornado.gen.coroutine
def confirm_put_handler(req):
    uuid = req['query']['uuid']
    checksum = req['query']['checksum']
    proc = _futures[uuid]
    proc.wait()
    assert proc.returncode == 0
    local_checksum = proc.stderr.read().decode('utf-8').strip()
    assert checksum == local_checksum, [checksum, local_checksum]
    # print([checksum, resp['stderr']])
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

def server(port=8000, debug=False):
    routes = [
        ('/prepare_put', {'post': prepare_put_handler}),
        ('/confirm_put', {'post': confirm_put_handler}),
        ('/prepare_get', {'post': prepare_get_handler}),
        ('/confirm_get', {'post': confirm_get_handler}),
        ('/delete',      {'post': delete_handler}),
        ('/list',        {'get':  list_handler}),
    ]
    web.app(routes, debug=debug).listen(port)
    tornado.ioloop.IOLoop.current().start()

def main():
    util.log.setup()
    argh.dispatch_command(server)
