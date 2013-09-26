import random
import tempfile

import requests
from gevent.event import AsyncResult
from gevent.monkey import patch_all
from nose.tools import eq_

from spinoff.actor import Node, Actor
from spinoff.util.python import deferred_cleanup
from spinoff.contrib.http.server import HttpServer
from spinoff.util.testing.common import assert_raises


patch_all()  # to make python-requests cooperative


@deferred_cleanup
def test_basic(defer):
    node = Node()
    defer(node.stop)

    responders = []
    http_server = node.spawn(HttpServer.using(address=('localhost', 0), responders=responders))
    _, port = actor_exec(node, lambda: http_server.ask('get-addr'))

    eq_(404, requests.get('http://localhost:%d' % (port,)).status_code)

    rnd_response = str(random.random())
    responders[:] = [
        (r'^/$', make_responder(lambda request: request.write(rnd_response))),
    ]
    eq_(200, requests.get('http://localhost:%d' % (port,)).status_code)


@deferred_cleanup
def test_urlmatch_with_extra_args_to_responder(defer):
    node = Node()
    defer(node.stop)

    responder = make_responder(lambda req, extra: req.write(str(extra)))
    rnd_data = bytes(random.random())
    responders = [(r'', responder.using(extra=rnd_data))]
    http_server = node.spawn(HttpServer.using(address=('localhost', 0), responders=responders))
    _, port = actor_exec(node, lambda: http_server.ask('get-addr'))

    eq_(rnd_data, requests.get('http://localhost:%d' % (port,)).text)


@deferred_cleanup
def test_file_upload(defer):
    node = Node()
    defer(node.stop)

    def handle_file(request):
        request.write(request.files['file'].read())

    responders = [(r'^/handle-file$', make_responder(handle_file))]
    http_server = node.spawn(HttpServer.using(address=('localhost', 0), responders=responders))
    _, port = actor_exec(node, lambda: http_server.ask('get-addr'))

    rnd_data = bytes(random.random())
    with tempfile.NamedTemporaryFile() as f:
        f.write(rnd_data)
        f.flush()
        req = requests.post('http://localhost:%d/handle-file' % (port,), files={'file': open(f.name, 'rb')})
    eq_(200, req.status_code)
    eq_(rnd_data, req.text)


@deferred_cleanup
def test_stop(defer):
    node = Node()
    defer(node.stop)

    responders = []
    http_server = node.spawn(HttpServer.using(address=('localhost', 0), responders=responders))
    _, port = actor_exec(node, lambda: http_server.ask('get-addr'))

    eq_(404, requests.get('http://localhost:%d' % (port,)).status_code)

    http_server.stop()

    with assert_raises(requests.ConnectionError):
        requests.get('http://localhost:%d' % (port,))


def make_responder(fn):
    class Responder(Actor):
        def run(self, *args, **kwargs):
            return fn(*args, **kwargs)
    return Responder


def actor_exec(node, fn, *args, **kwargs):
    class ExecActor(Actor):
        def run(self):
            ret.set(fn(*args, **kwargs))
    ret = AsyncResult()
    node.spawn(ExecActor)
    return ret.get()
