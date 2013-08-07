from gevent import sleep, with_timeout

from spinoff.actor import Actor
from spinoff.util.logging import dbg


class ExampleProcess(Actor):
    def run(self, other_actor):
        if isinstance(other_actor, str):
            other_actor = self.node.lookup_str(other_actor)
        while True:
            dbg("sending greeting to %r" % (other_actor,))
            other_actor << 'hello!'

            dbg("waiting for ack from %r" % (other_actor,))
            with_timeout(5.0, self.get, 'ack')

            dbg("got 'ack' from %r; now sleeping a bit..." % (other_actor,))
            sleep(1.0)


class ExampleActor(Actor):
    def pre_start(self):
        dbg("starting")

    def receive(self, msg):
        dbg("%r from %r" % (msg, self.sender))
        self.sender << 'ack'

    def post_stop(self):
        dbg("stopping")
