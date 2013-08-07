from gevent import sleep, with_timeout
from spinoff.actor import Actor
from spinoff.util.logging import dbg


class ExampleProcess(Actor):
    def run(self):
        child = self.spawn(ExampleActor)

        while True:
            dbg("sending greeting to %r" % (child,))
            child << 'hello!'

            dbg("waiting for ack from %r" % (child,))
            with_timeout(5.0, self.get, 'ack')

            dbg("got 'ack' from %r; now sleeping a bit..." % (child,))
            sleep(1.0)


class ExampleActor(Actor):
    def pre_start(self):
        dbg("starting")

    def receive(self, msg):
        dbg("%r from %r" % (msg, self.sender))
        self.sender << 'ack'

    def post_stop(self):
        dbg("stopping")
