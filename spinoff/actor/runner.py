from __future__ import print_function

import re

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.python.failure import Failure
from txzmq import ZmqFactory, ZmqRouterConnection

from spinoff.actor import spawn, Actor, Props, set_default_node, Node
from spinoff.actor._actor import _validate_nodeid, _VALID_IP_RE
from spinoff.actor.remoting import Hub, HubWithNoRemoting
from spinoff.util.logging import log, err, panic
from spinoff.util.pattern_matching import ANY


_EMPTY = object()


class ActorRunner(Service):

    def __init__(self, actor_cls, init_params={}, initial_message=_EMPTY, nodeid=None, name=None):
        if nodeid:
            _validate_nodeid(nodeid)
        self._init_params = init_params
        self._initial_message = initial_message
        self._actor_cls = actor_cls
        self._wrapper = None
        self._nodeid = nodeid
        self._name = name

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        log("Running: %s" % (actor_path,), "@ /%s" % (self._name,) if self._name else '')

        def start_actor():
            if self._nodeid:
                if not re.match(_VALID_IP_RE, self._nodeid):
                    err("Node ID to IP resolution not supported yet; "
                        "provide an <ip-addr:port> parameter to set up remoting")
                    reactor.stop()
                    return

                log("Setting up remoting; node ID =", self._nodeid)
                ip_addr = self._nodeid
                f1 = ZmqFactory()
                insock = ZmqRouterConnection(f1, identity='tcp://%s' % (ip_addr,))
                outsock = ZmqRouterConnection(f1, identity='tcp://%s' % (ip_addr,))
                hub = Hub(insock, outsock, node=self._nodeid)
            else:
                log("No remoting requested; specify `--remoting/-r <nodeid>` (nodeid=host:port) to set up remoting")
                hub = HubWithNoRemoting()

            set_default_node(Node(hub=hub))

            try:
                self._wrapper = spawn(Props(Wrapper, Props(self._actor_cls, **self._init_params), spawn_at=self._name), name='_runner')
            except Exception:
                panic("Failed to start wrapper for %s\n" % (actor_path,),
                      Failure().getTraceback())
                reactor.stop()
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
    def __init__(self, actor_factory, spawn_at):
        self.actor_factory = actor_factory
        self.spawn_at = spawn_at

    def pre_start(self):
        name = (self.spawn_at
                if self.spawn_at else
                self.actor_factory.__name__
                if isinstance(self.actor_factory, type) else
                self.actor_factory.cls.__name__)
        self.actor = self.watch(self.node.spawn(self.actor_factory, name=name))
        log("Spawned %s" % (self.actor,))

    def receive(self, message):
        if message == ('_forward', ANY):
            _, payload = message
            self.actor << payload
        elif message == ('terminated', ANY):
            log("Actor terminated, stopping...")
            _, actor = message
            assert actor == self.actor
            self.stop()
        else:
            log("Actor sent message to parent: %r" % (message,))

    def post_stop(self):
        try:
            reactor.stop()
        except ReactorNotRunning:
            pass
