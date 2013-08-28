import random

from gevent import sleep

from spinoff.actor import Actor
from spinoff.contrib.http import HttpServer


class Main(Actor):
    def run(self):
        http_srv = self.spawn(HttpServer.using(address=('localhost', 8080), responders=[
            (r'^/$', IndexResponder),
            (r'^/foo/(?P<name>.+)$', FooResponder),
            (r'^/add/(?P<a>[0-9]+)/(?P<b>[0-9]+)$', AdderResponder),
            (r'^/form$', FormSubmitResponder),
            (r'^/file-echo$', FileEchoResponder),
        ]))
        http_srv.join()


class IndexResponder(Actor):
    def run(self, request):
        request.write('Hello, World!\n')


class FooResponder(Actor):
    def run(self, request, name):
        request.write('foo got: %s\n' % (name,))
        request.write('...and adder computed: 3 + 4 = %s\n' % (self.spawn(Adder).ask((3, 4)),))


class Adder(Actor):
    def receive(self, msg):
        a, b = msg
        sleep(0.2)  # processing time
        self.reply(a + b)


class AdderResponder(Actor):
    def run(self, request, a, b):
        a, b = int(a), int(b)
        request.write('%d + %d = %d\n' % (a, b, self.spawn(Adder).ask((a, b)),))


class FormSubmitResponder(Actor):
    def run(self, request):
        stuff = request.form.getlist('stuff')
        for item in stuff:
            if random.random() < 0.25:
                request.set_status('400 Bad Request')
                request.writeln("I'm feeling moody...")
                return
        request.writeln("got:\n%s" % ("\n".join(str(x) for x in stuff),))


class FileEchoResponder(Actor):
    def run(self, request):
        fl = request.files['file1']
        request.start_response('200 OK', [('Content-Disposition', 'attachment; filename="%s-echo"' % (str(fl.filename),))])
        while True:
            chunk = fl.read(50 * 1024)
            if not chunk:
                break
            request.write(chunk)
