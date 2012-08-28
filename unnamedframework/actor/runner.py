from __future__ import print_function

import sys

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.python import log
from twisted.python.failure import Failure

from unnamedframework.actor import spawn, Actor, Props
from unnamedframework.util.pattern_matching import ANY
from unnamedframework.actor.supervision import Default


_EMPTY = object()


class ActorRunner(Service):

    def __init__(self, actor_cls, initial_message=_EMPTY):
        self._initial_message = initial_message
        self._actor_cls = actor_cls
        self._wrapper = None

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        log.msg("*** Running: %s" % (actor_path,))

        def start_actor():
            try:
                self._wrapper = spawn(Props(Wrapper, self._actor_cls), name='wrapper')
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


class Wrapper(Actor):
    def __init__(self, actor_cls):
        self.actor_cls = actor_cls

    def supervise(self, exc):
        print("*** Actor failed: %r" % (exc,), file=sys.stderr)
        return Default

    def pre_start(self):
        self.actor = self.watch(self.spawn(self.actor_cls))

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
