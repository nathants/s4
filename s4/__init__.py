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

def rand_port():
    return random.randint(2000, 65000)

@tornado.gen.coroutine
def prepare_put_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

@tornado.gen.coroutine
def confirm_put_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

@tornado.gen.coroutine
def prepare_get_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

@tornado.gen.coroutine
def confirm_get_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

@tornado.gen.coroutine
def list_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

@tornado.gen.coroutine
def delete_handler(req):
    yield tornado.gen.moment
    return {'status': 200}

def server(port=8000, debug=False):
    routes = [
        ('/prepare_put', {'post': prepare_put_handler}),
        ('/confirm_put', {'post': confirm_put_handler}),
        ('/prepare_get', {'post': prepare_get_handler}),
        ('/confirm_get', {'post': confirm_get_handler}),
        ('/list',        {'get': list_handler}),
        ('/delete',      {'post': delete_handler}),
    ]
    web.app(routes, debug=debug).listen(port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    util.log.setup()
    argh.dispatch_commands([server])
