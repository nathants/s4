import s4
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

_ports_in_use = set()

_futures = {}

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
    yield pool.thread.submit(shell.run, 'mkdir -p', parent)
    future = pool.thread.submit(shell.run, 'timeout 120 nc -w 0 -q 0 -l', port, '| xxhsum >', path, warn=True) # TODO timeout is a bit crude, do something else?
    uuid = new_uuid()
    _futures[uuid] = future
    return {'status': 200, 'body': json.dumps([uuid, port])}

@tornado.gen.coroutine
def confirm_put_handler(req):
    uuid = req['query']['uuid']
    checksum = req['query']['checksum']
    resp = yield tornado.gen.with_timeout(datetime.timedelta(seconds=120), _futures[uuid])
    assert resp['exitcode'] == 0, resp
    assert checksum == resp['stderr'], [checksum, resp['stderr']]
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

if __name__ == '__main__':
    util.log.setup()
    argh.dispatch_commands([server])
