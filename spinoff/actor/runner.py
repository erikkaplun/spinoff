from __future__ import print_function

import re
import traceback

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure
from txzmq import ZmqFactory, ZmqPushConnection, ZmqPullConnection

from spinoff.actor import spawn, Actor, set_default_node, Node
from spinoff.actor._actor import _validate_nodeid, _VALID_IP_RE
from spinoff.actor.remoting import Hub, HubWithNoRemoting
from spinoff.util.logging import log, err, panic
from spinoff.util.async import after
from spinoff.util.pattern_matching import ANY
from spinoff.actor.events import Events, Message
from spinoff.actor.supervision import Stop, Restart, Resume

_EMPTY = object()


class ActorRunner(Service):

    def __init__(self, actor_cls, init_params={}, initial_message=_EMPTY, nodeid=None, name=None, supervise='stop', keep_running=False):
        if nodeid:
            _validate_nodeid(nodeid)
        self._init_params = init_params
        self._initial_message = initial_message
        self._actor_cls = actor_cls
        self._wrapper = None
        self._nodeid = nodeid
        self._name = name
        self._supervise = supervise
        self._keep_running = keep_running

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        Events.log(Message("Running: %s%s" % (actor_path, " @ /%s" % (self._name,) if self._name else '')))

        def start_actor():
            if self._nodeid:
                if not re.match(_VALID_IP_RE, self._nodeid):
                    err("Node ID to IP resolution not supported yet; "
                        "provide an <ip-addr:port> parameter to set up remoting")
                    reactor.stop()
                    return

                Events.log(Message("Setting up remoting; node ID = %s" % (self._nodeid,)))
                try:
                    f1 = ZmqFactory()
                    insock = ZmqPullConnection(f1)
                    outsock = lambda: ZmqPushConnection(f1, linger=0)
                    hub = Hub(insock, outsock, nodeid=self._nodeid)
                except Exception:
                    err("Could not set up remoting")
                    traceback.print_exc()
                    reactor.stop()
                    return
            else:
                Events.log(Message("No remoting requested; specify `--remoting/-r <nodeid>` (nodeid=host:port) to set up remoting"))
                hub = HubWithNoRemoting()

            supervision = {'stop': Stop, 'restart': Restart, 'resume': Resume}[self._supervise]
            set_default_node(Node(hub=hub, root_supervision=supervision))

            try:
                self._wrapper = spawn(Wrapper.using(self._actor_cls.using(**self._init_params),
                                                    spawn_at=self._name, keep_running=self._keep_running), name='_runner')
            except Exception:
                panic("Failed to start wrapper for %s\n" % (actor_path,),
                      Failure().getTraceback())
                reactor.stop()
            else:
                if self._initial_message is not _EMPTY:
                    self._wrapper << ('_forward', self._initial_message)

        reactor.callLater(0.0, start_actor)

    @inlineCallbacks
    def stopService(self):
        yield Node.stop_all()

    def __repr__(self):
        return '<ActorRunner>'


class Wrapper(Actor):
    def __init__(self, actor_factory, spawn_at, keep_running):
        self.actor_factory = actor_factory
        self.spawn_at = spawn_at
        self.keep_running = keep_running

    def _do_spawn(self):
        self.actor = self.watch(self.node.spawn(self.actor_factory, name=self.spawn_at))
        Events.log(Message("Spawned %s" % (self.actor,)))

    def pre_start(self):
        self._do_spawn()

    def receive(self, message):
        if message == ('_forward', ANY):
            _, payload = message
            self.actor << payload
        elif message == ('terminated', self.actor):
            _, actor = message
            if self.keep_running:
                after(1.0).do(self._do_spawn)
            else:
                log("actor terminated but not re-spawning actor; pass --keepruning/-k to change this behaviour")
                self.stop()
        else:
            Events.log(Message("Contained actor sent a message to parent: %r" % (message,)))

    def post_stop(self):
        try:
            reactor.stop()
        except ReactorNotRunning:
            pass

    def __repr__(self):
        return '#runner#'
