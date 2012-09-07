from __future__ import print_function

import sys

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.python import log
from twisted.python.failure import Failure

from spinoff.actor import spawn, Actor, Props
from spinoff.util.pattern_matching import ANY


_EMPTY = object()


class ActorRunner(Service):

    def __init__(self, actor_cls, init_params={}, initial_message=_EMPTY):
        self._init_params = init_params
        self._initial_message = initial_message
        self._actor_cls = actor_cls
        self._wrapper = None

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        log.msg("*** Running: %s" % (actor_path,))

        def start_actor():
            try:
                self._wrapper = spawn(Props(Wrapper, Props(self._actor_cls, **self._init_params)), name='_runner')
            except Exception:
                print("*** Failed to start wrapper for %s\n" % (actor_path,), file=sys.stderr)
                Failure().printTraceback(file=sys.stderr)
                return
            else:
                if self._initial_message is not _EMPTY:
                    self._wrapper << ('_forward', self._initial_message)

        reactor.callLater(0.0, start_actor)

    def stopService(self):
        if self._wrapper:
            self._wrapper.stop()
            return self._wrapper.join()

    def __repr__(self):
        return '<ActorRunner>'


class Wrapper(Actor):
    def __init__(self, actor_factory):
        self.actor_factory = actor_factory

    def pre_start(self):
        name = (self.actor_factory.__name__
                if isinstance(self.actor_factory, type) else
                self.actor_factory.cls.__name__)
        self.actor = self.watch(self.node.spawn(self.actor_factory, name=name))

    def receive(self, message):
        if message == ('_forward', ANY):
            _, payload = message
            self.actor << payload
        elif message == ('terminated', ANY):
            _, actor = message
            assert actor == self.actor
            self.stop()
        else:
            print("*** Actor sent message to parent: %r" % (message,), file=sys.stderr)

    def post_stop(self):
        try:
            reactor.stop()
        except ReactorNotRunning:
            pass
