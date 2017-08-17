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

_procs = {} # TODO need to cleanup entries here created by /prepare_put left dangling by never calling /confirm_put

def new_uuid():
    for _ in range(10):
        val = str(uuid.uuid4())
        if val not in _procs:
            _procs[val] = '::taken'
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
    # TODO if too many procs already open, send a backoff signal to the client, ie sleep and retry
    key = req['query']['key']
    assert key.startswith('s3://')
    assert not key.endswith('/')
    assert '0.0.0.0' == s4.cli.pick_server(key) # make sure the key is meant to live on this server
    path = key.split('s3://')[-1]
    parent = os.path.dirname(path)
    port = new_port()
    yield pool.thread.submit(shell.run, 'mkdir -p', parent)
    cmd = f'timeout 120 bash -c "nc -q 0 -l {port} | xxhsum > {path}"'
    shell.run(f'timeout 120 bash -c "while netstat -ln|grep {port}; do sleep .1; done"')
    uuid = new_uuid()
    _procs[uuid] = subprocess.Popen(cmd, shell=True, executable='/bin/bash', stderr=subprocess.PIPE)
    return {'status': 200, 'body': json.dumps([uuid, port])}

@tornado.gen.coroutine
def confirm_put_handler(req):
    uuid = req['query']['uuid']
    checksum = req['query']['checksum']
    proc = _procs.pop(uuid)
    while proc.poll() is None:
        yield tornado.gen.sleep(.05)
    assert proc.returncode == 0
    local_checksum = proc.stderr.read().decode('utf-8').strip()
    assert checksum == local_checksum, [checksum, local_checksum]
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
