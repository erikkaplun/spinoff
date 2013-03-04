from __future__ import print_function

from twisted.internet.error import ReactorAlreadyInstalledError
from geventreactor import install, GeventReactor
try:
    install()
except ReactorAlreadyInstalledError:
    from twisted.internet import reactor
    assert isinstance(reactor, GeventReactor)

from gevent import spawn, spawn_later
from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.python.failure import Failure

from spinoff.actor import Actor, Node
from spinoff.actor.validate import _validate_nodeid
from spinoff.util.logging import panic, dbg, log
from spinoff.util.pattern_matching import ANY

_EMPTY = object()


class ActorRunner(Service):
    def __init__(self, actor_cls, init_params={}, initial_message=_EMPTY, nodeid=None, name=None, keep_running=False):
        if nodeid:
            _validate_nodeid(nodeid)
        self._init_params = init_params
        self._initial_message = initial_message
        self._actor_cls = actor_cls
        self._wrapper = None
        self._nodeid = nodeid
        self._name = name
        self._keep_running = keep_running

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        def start_actor():
            # if self._nodeid:
            #     log("Setting up remoting; node ID = %s" % (self._nodeid,))
            # else:
            #     log("No remoting requested; specify `--remoting/-r <nodeid>` (nodeid=host:port) to set up remoting")

            self.node = Node(nid=self._nodeid, enable_remoting=True if self._nodeid else False)

            try:
                self._wrapper = self.node.spawn(Wrapper.using(
                    self._actor_cls.using(**self._init_params),
                    spawn_at=self._name, keep_running=self._keep_running
                ), name='_runner')
            except Exception:
                panic("Failed to start wrapper for %s\n" % (actor_path,),
                      Failure().getTraceback())
                reactor.stop()
            else:
                if self._initial_message is not _EMPTY:
                    self._wrapper << ('_forward', self._initial_message)
        # dbg("Running: %s%s" % (actor_path, " @ /%s" % (self._name,) if self._name else ''))
        spawn(start_actor).link_exception(lambda _: reactor.stop())

    def stopService(self):
        if getattr(self, 'node', None):
            self.node.stop()

    def __repr__(self):
        return '<ActorRunner>'


class Wrapper(Actor):
    def __init__(self, actor_factory, spawn_at, keep_running):
        self.actor_factory = actor_factory
        self.spawn_at = spawn_at
        self.keep_running = keep_running

    def _do_spawn(self):
        self.actor = self.watch(self.node.spawn(self.actor_factory, name=self.spawn_at))

    def pre_start(self):
        self._do_spawn()

    def receive(self, message):
        if message == ('_forward', ANY):
            _, payload = message
            self.actor << payload
        elif message == ('terminated', self.actor):
            _, actor = message
            if self.keep_running:
                spawn_later(1.0, self._do_spawn)
            else:
                self.stop()
        else:
            log("Contained actor sent a message to parent: %r" % (message,))

    def post_stop(self):
        try:
            reactor.callFromGreenlet(reactor.stop)
        except ReactorNotRunning:
            pass

    def __repr__(self):
        return '#runner#'
