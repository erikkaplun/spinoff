from __future__ import print_function

import datetime
import json
import os

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.web import static
from twisted.web.resource import Resource, ErrorPage
from twisted.web.server import NOT_DONE_YET, Site
from genshi.template import TemplateLoader

from spinoff.util.async import with_timeout
from spinoff.util.pattern_matching import ANY


DEFAULT_ASK_TIMEOUT = 5.0


def run_server(monitor):
    site = Site(MonitorResource(monitor))
    return reactor.listenTCP(9080, site)


class ResourceBase(Resource):
    def __init__(self, actor):
        Resource.__init__(self)
        self.actor = actor

    def _child(self, cls):
        return cls(self.actor)

    def render(self, *args, **kwargs):
        ret = Resource.render(self, *args, **kwargs)
        if ret is not NOT_DONE_YET:
            ret = json.dumps(ret, indent=2) if not isinstance(ret, basestring) else ret
            if not ret.endswith('\n'):
                ret += '\n'
        return ret

    def ask(self, q, request, arg=None, timeout=DEFAULT_ASK_TIMEOUT):
        d = with_timeout(5.0, Deferred())
        self.actor << (q, d, arg)
        request.setHeader('Content-Type', 'text/plain')
        d.addCallback(lambda result: (
            request.write(json.dumps({'success': True, 'result': result}, indent=4) + '\n'),
            request.finish(),
        ))
        d.addErrback(lambda f: (
            request.write(json.dumps({'success': False, 'error': {'traceback': f.getTraceback()}}, indent=4) + '\n'),
            request.finish(),
        ))
        return NOT_DONE_YET

    def ask_html(self, q, template, request, arg=None, process=lambda x: x, timeout=DEFAULT_ASK_TIMEOUT):
        d = with_timeout(5.0, Deferred())
        self.actor << (q, d, arg)
        # request.setHeader('Content-Type', 'text/plain')
        d.addCallback(lambda result: (
            # TemplateLoader(os.path.join(os.path.dirname(__file__), 'templates'), auto_reload=True).load(template + '.html').generate(result=result).render('html', doctype='html')
            request.write(TemplateLoader(os.path.join(os.path.dirname(__file__), 'templates'), auto_reload=True).load(template + '.html').generate(result=process(result)).render('html', doctype='html')),
            request.finish(),
        ))
        d.addErrback(lambda f: (
            request.setHeader('Content-Type', 'text/plain'),
            request.write(f.getTraceback() + '\n'),
            request.finish(),
        ))
        return NOT_DONE_YET


class MonitorResource(ResourceBase):
    # isLeaf = True

    CHILDREN = property(lambda self: {
        'up': UpResource,
        'state': StateResource,
        'log': LogResource,

        'dashboard': DashboardResource,
        'dashboard.html': HtmlDashboardResource,
        'favicon.ico': lambda _: ErrorPage(status=404, brief="Not found: favicon.ico", detail=""),
    })

    def __init__(self, *args, **kwargs):
        ResourceBase.__init__(self, *args, **kwargs)
        self.putChild('static', static.File(os.path.join(os.path.dirname(__file__), 'static')))

    def render_GET(self, request):
        return {'child-resources': ['%s/' % (x,) for x in self.CHILDREN.keys()]}

    def getChild(self, path, request):
        if path in self.CHILDREN:
            return self._child(self.CHILDREN[path])
        return ErrorPage(status=404, brief="Not found: %r" % (path,),
                         detail="The resource %r you requested was not found" % (path,))


class UpResource(ResourceBase):
    isLeaf = True

    def render_GET(self, request):
        return self.ask('get-up', arg=ANY, request=request)


class StateResource(ResourceBase):
    isLeaf = True

    def render_GET(self, request):
        return self.ask('get-state', arg=ANY, request=request)


class LogResource(ResourceBase):
    isLeaf = True

    def render_GET(self, request):
        return self.ask('get-log', arg=ANY, request=request)


class DashboardResource(ResourceBase):
    isLeaf = True

    def render_GET(self, request):
        return self.ask('get-all', arg=ANY, request=request)


class HtmlDashboardResource(ResourceBase):
    isLeaf = True

    def render_GET(self, request):
        def process(result):
            for uri, status, seen, state in reversed(sorted(result, key=lambda x: x[0])):
                yield (uri, status, datetime.datetime.utcfromtimestamp(seen), state)

        return self.ask_html('get-all', arg=ANY, template='dashboard', process=lambda x: list(process(x)), request=request)
