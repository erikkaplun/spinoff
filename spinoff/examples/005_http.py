from gevent import sleep

from spinoff.actor import Actor
from spinoff.contrib.http.server import HttpServer


class Main(Actor):
    def run(self):
        http_srv = self.spawn(HttpServer.using(address=('localhost', 8080), responders=[
            (r'^/$', IndexResponder),
            (r'^/foo$', FooResponder),
        ]))
        http_srv.join()


class IndexResponder(Actor):
    def run(self, request):
        request.start_response('200 OK', [('Content-Type', 'text/html')])
        request.write('index')
        request.close()


class FooResponder(Actor):
    def run(self, request):
        request.start_response('200 OK', [('Content-Type', 'text/html')])
        request.write('foo\n')
        self.spawn(SubResponder.using(request))


class SubResponder(Actor):
    def run(self, req):
        sleep(1.0)
        req.write('sub\n')
        req.close()
