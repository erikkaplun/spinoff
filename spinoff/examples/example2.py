from spinoff.actor import Actor, lookup
from spinoff.actor.process import Process
from spinoff.util.logging import dbg
from spinoff.util.async import sleep, with_timeout


class ExampleProcess(Process):
    def run(self, other_actor):
        other_actor = lookup(other_actor) if isinstance(other_actor, str) else other_actor
        while True:
            dbg("sending greeting to %r" % (other_actor,))
            other_actor << ('hello!', self.ref)

            dbg("waiting for ack from %r" % (other_actor,))
            yield with_timeout(5.0, self.get('ack'))

            dbg("got 'ack' from %r; now sleeping a bit..." % (other_actor,))
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
