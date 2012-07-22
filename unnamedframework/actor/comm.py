from __future__ import print_function

import sys
import pickle
import weakref

from twisted.internet.defer import succeed

from unnamedframework.actor import Actor, UnhandledMessage
from unnamedframework.actor.transport.zeromq import ZmqRouter, ZmqDealer
from unnamedframework.util.async import sleep
from unnamedframework.util.pattern_matching import IGNORE, ANY


class ActorRef(object):

    _comm = None

    def __init__(self, referee):
        assert isinstance(referee, (basestring, Actor))
        self._referee = referee if not isinstance(referee, basestring) else None
        self._addr = referee if isinstance(referee, basestring) else None

        self._assign_comm()

    @property
    def addr(self):
        if not self._addr:
            assert self._referee
            comm = self._get_comm()
            if not comm:
                return None
            else:
                self._addr = comm._get_addr(self._referee)
                assert self._addr
        return self._addr

    def _assign_comm(self):
        # assigning a fixed comm to an ActorRef is purely for better
        # testability, or more precisely, for consistency during
        # testing--this way it is not possible to use the same
        # ActorRef instance with different comms.
        if Comm._overridden:
            self._comm = Comm._overridden

    def _get_comm(self):
        if not self._comm:
            if Comm._overridden:
                self._comm = Comm._overridden
            else:
                return Comm._current
        return self._comm

    def send(self, message):
        if self._referee:
            self._referee._send(message)
        else:
            assert self._addr
            local_actor = self._get_comm().send_msg(self._addr, message)
            if local_actor:
                self._referee = local_actor
                local_actor._send(message)

    def __getstate__(self):
        return self.addr

    def __setstate__(self, data):
        self._addr = data
        self._referee = None
        self._assign_comm()

    def __eq__(self, other):
        return isinstance(other, ActorRef) and (self._referee == other._referee if self._referee and other._referee else self.addr == other.addr)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return '<ActorRef @ %s>' % (self.addr or 'local', )


class Comm(Actor):

    _current = None
    _overridden = None  # only for testing

    def receive(self, message):
        if message == ('error', IGNORE(ANY), IGNORE(ANY)):
            raise RuntimeError("comm failed", message[2])
        elif isinstance(message, str):
            with self:
                actor_id, payload = pickle.loads(message)
            if actor_id not in self._registry_rev:
                print("received message for actor %s which does not exist (anymore)" % actor_id, file=sys.stderr)
            else:
                self._registry_rev[actor_id]._send(payload)
            return
        else:
            raise UnhandledMessage

    @classmethod
    def get_for_thread(self):
        return self._current if not self._overridden else self._overridden

    def __init__(self, addr, sock=None):
        assert not (addr.startswith('ipc://') or addr.startswith('inproc://')), \
            "ipc:// and inproc:// are not supported (yet?)"

        super(Comm, self).__init__()

        self._registry = {}
        self._registry_rev = {}
        self._connections = set()

        self.addr = addr

        self._mock_sock = sock

    def _before_start(self):
        if self._mock_sock:  # this is purely for testability
            self._outgoing_sock = self._spawn(self._mock_sock)
            # no incoming sock needed when mocks are used
        else:
            self._outgoing_sock = self._spawn(ZmqRouter(endpoint=None))
            # incoming
            self._spawn(ZmqDealer(endpoint=('bind', self.addr),
                                  identity=self.addr))

    def __enter__(self):
        assert not Comm._overridden
        Comm._overridden = self
        return self

    def __exit__(self, *args):
        assert Comm._overridden
        Comm._overridden = None

    def _get_addr(self, actor):
        if actor in self._registry:
            actor_id = self._registry[actor]
        else:
            actor_id = str(id(actor))
            self._registry[actor] = actor_id
            self._registry_rev[actor_id] = actor

        return '%s/%s' % (self.addr, actor_id)

    def send_msg(self, actor_addr, msg):
        if not (actor_addr.startswith('tcp://') or actor_addr.startswith('udp://')):
            raise ValueError("Actor addresses should start with tcp:// or udp://")
        if '/' not in actor_addr[len('___://'):]:
            raise ValueError("Actor addresses should have an actor ID")

        node_addr, actor_id = actor_addr.rsplit('/', 1)

        if node_addr == self.addr:
            return self._registry_rev[actor_id]
        else:
            self.ensure_connected(to=node_addr).addCallback(
                lambda _: self._outgoing_sock._send((node_addr, pickle.dumps((actor_id, msg)))))

    def ensure_connected(self, to):
        if not self._mock_sock and not self._zmq_is_connected(to=to):
            self._outgoing_sock.add_endpoints([('connect', to)])
            self._connections.add(to)
            return sleep(0.005)
        else:
            return succeed(None)

    def set_id(self, ref, id):
        assert isinstance(ref, ActorRef)
        assert ref._referee, "Can only set ID for local actors"
        actor = ref._referee
        if actor in self._registry:
            raise RuntimeError("actor already registered")
        self._registry[actor] = id
        self._registry_rev[id] = actor

    def _zmq_is_connected(self, to):
        return to in self._connections

    def __repr__(self):
        return '<Comm %s>' % (self.addr, )


@property
def ref(self):
    if not (self._ref and self._ref()):
        ref = ActorRef(self)
        self._ref = weakref.ref(ref)
        return ref
    else:
        return self._ref()


def set_id(self, id):
    Comm.get_for_thread().set_id(self, id)


Actor.ref = ref
Actor.set_id = set_id
