import re
import traceback

from gevent.pywsgi import WSGIServer
from gevent.queue import Channel

from spinoff.actor import Actor
from spinoff.actor.events import Events, Error
from spinoff.actor.exceptions import Unhandled
from spinoff.util.logging import dbg
from spinoff.util.pattern_matching import ANY


class HttpServer(Actor):
    def pre_start(self, address, responders, default_content_type='text/html'):
        self.responders = responders
        self.default_content_type = default_content_type
        self.server = WSGIServer(address, self.handle_wsgi_request)
        self.server.start()

    def handle_wsgi_request(self, env, start_response):
        ch = Channel()
        req = Request(ch, env, start_response, default_content_type=self.default_content_type)
        self << ('handle', req)
        return response_stream(ch)

    def receive(self, msg):
        if ('handle', ANY) == msg:
            _, req = msg
            responder = self.get_responder(req.env['PATH_INFO'])
            if responder:
                self.spawn(RequestHandler.using(req, responder))
            else:
                req.start_response('404 Not Found', [('Content-Type', 'text/html')])
                req.write('<h1>404 Not Found</h1>\n')
                req.write('The page you tried to reach could not be found.\n')
                req.close()

        elif 'get-addr' == msg:
            self.reply(self.server.address)
        else:
            raise Unhandled

    def get_responder(self, path):
        for pattern, responder in self.responders:
            m = re.compile(pattern).match(path)
            if m:
                args = m.groups()
                kwargs = m.groupdict()
                return responder, args if not kwargs else (), kwargs


class RequestHandler(Actor):
    def run(self, req, responder):
        responder, args, kwargs = responder
        self.responder = responder = self.spawn(responder.using(req, *args, **kwargs))
        self.error = None
        try:
            Events.subscribe(Error, self.check_error)  # XXX: it would be nicer if Events.subscribe accepted an actor ref to filter on
            responder.join()
            if not req.closed:
                if self.error:
                    req.start_response('500 Internal Server Error', [('Content-Type', 'text/html')])
                    req.write(''.join(traceback.format_exception(type(self.error.exc), self.error.exc, self.error.tb)))
                req.close()
        finally:
            Events.unsubscribe(Error, self.check_error)

    def check_error(self, error):
        if error.actor == self.responder:
            self.error = error


class _BREAK(object):
    def __repr__(self):
        return '_BREAK'
_BREAK = _BREAK()


def response_stream(ch):
    while True:
        val = ch.get()
        if val is _BREAK:
            break
        yield val


class Request(object):
    def __init__(self, ch, env, start_response, default_content_type):
        self.ch = ch
        self.env = env
        self._response_started = False
        self._start_response = start_response
        self.default_content_type = default_content_type
        self.closed = False

    def set_status(self, status):
        self.start_response(status, [('Content-Type', self.default_content_type)])

    def start_response(self, *args):
        self._response_started = True
        ret = self._start_response(*args)
        self.start_response = self._start_response  # optimization
        return ret

    def write(self, data):
        if not self._response_started:
            self._start_response('200 OK', [('Content-Type', 'text/html')])
        self.ch.put(data)

    def close(self):
        self.closed = True
        self.ch.put(_BREAK)
