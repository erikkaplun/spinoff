from spinoff.actor import Actor
from spinoff.actor.process import Process
from spinoff.util.logging import dbg
from spinoff.util.async import sleep, with_timeout


class ExampleProcess(Process):
    def run(self):
        child = self.spawn(ExampleActor)

        while True:
            dbg("sending greeting to %r" % (child,))
            child << ('hello!', self.ref)

            dbg("waiting for ack from %r" % (child,))
            yield with_timeout(5.0, self.get('ack'))

            dbg("got 'ack' from %r; now sleeping a bit..." % (child,))
            yield sleep(1.0)


class ExampleActor(Actor):
    def pre_start(self):
        dbg("starting")

    def receive(self, msg):
        content, sender = msg
        dbg("%r from %r" % (content, sender))
        sender << 'ack'

    def post_stop(self):
        dbg("stopping")
