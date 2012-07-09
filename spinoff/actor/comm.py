from __future__ import print_function

import sys
import pickle

from twisted.internet.defer import succeed

from spinoff.actor import BaseActor
from spinoff.actor.transport.zeromq import ZmqRouter, ZmqDealer
from spinoff.util.async import sleep


BASE_PORT = 11000


class ActorRef(object):

    def __init__(self, referee):
        self._referee = referee if not isinstance(referee, basestring) else None
        self._addr = referee if isinstance(referee, basestring) else None

        # assigning a fixed comm to an ActorRef is purely for better
        # testability, or more precisely, for consistency during
        # testing--this way it is not possible to use the same
        # ActorRef instance with different comms.
        self._assign_comm()

    @property
    def addr(self):
        if not self._addr:
            assert self._referee
            self._addr = self.comm.get_addr(self._referee)
            assert self._addr
        return self._addr

    def _assign_comm(self):
        comm = Comm.get_for_thread()
        assert comm
        self.comm = comm

    def send(self, message):
        if self._referee:
            self._referee.send(message)
        else:
            assert self._addr
            local_actor = self.comm.send_msg(self._addr, message)
            if local_actor:
                self._referee = local_actor
                local_actor.send(message)

    def __getstate__(self):
        return self.addr

    def __setstate__(self, data):
        self._addr = data
        self._referee = None
        self._assign_comm()

    def __eq__(self, other):
        return self._referee == other._referee if self._referee and other._referee else self.addr == other.addr

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return '<ActorRef @ %s%s>' % (self.addr, '*' if self._referee else '')


class Comm(BaseActor):

    _current = None

    def handle(self, message):
        with self:
            actor_id, payload = pickle.loads(message)
        if actor_id not in self._registry_rev:
            print("received message for actor %s which does not exist (anymore)" % actor_id, file=sys.stderr)
        else:
            self._registry_rev[actor_id].send(payload)

    @classmethod
    def get_for_thread(self):
        assert self._current
        return self._current

    def __init__(self, host, process=1, sock=None):
        super(Comm, self).__init__()

        self._registry = {}
        self._registry_rev = {}
        self._connections = set()

        port = BASE_PORT + process - 1
        self.identity = '%s:%d' % (host, port)

        if sock:  # this is purely for testability
            self._outgoing_sock = self.spawn(sock)
            # no incoming sock needed when mocks are used
        else:
            self._outgoing_sock = self.spawn(ZmqRouter(endpoint=None))
            # incoming
            self.spawn(ZmqDealer(endpoint=('bind', _make_addr('*:%d' % port)),
                                 identity=self.identity))

    def install(self):
        assert not Comm._current
        Comm._current = self

    def uninstall(self):
        Comm._current = None

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *args):
        self.uninstall()

    def get_addr(self, actor):
        if actor in self._registry:
            actor_id = self._registry[actor]
        else:
            actor_id = str(id(actor))
            self._registry[actor] = actor_id
            self._registry_rev[actor_id] = actor

        return '%s/%s' % (_make_addr(self.identity), actor_id)

    def send_msg(self, addr, msg):
        try:
            _, identity_and_actor_id = addr.split('//', 1)
            identity, actor_id = identity_and_actor_id.split('/', 1)
        except ValueError:
            raise ValueError("invalid actor address: %s" % (addr, ))
        if identity == self.identity:
            return self._registry_rev[actor_id]
        else:
            self.ensure_connected(to=identity).addCallback(
                lambda _: self._outgoing_sock.send((identity, pickle.dumps((actor_id, msg)))))

    def ensure_connected(self, to):
        if isinstance(self._outgoing_sock, ZmqRouter) and not self._zmq_is_connected(to=to):
            self._outgoing_sock.add_endpoints([('connect', _make_addr(to))])
            self._connections.add(to)
            return sleep(0.005)
        else:
            return succeed(None)

    def set_id(self, actor, id):
        if actor in self._registry:
            raise RuntimeError("actor already registered")
        self._registry[actor] = id
        self._registry_rev[id] = actor

    def _zmq_is_connected(self, to):
        return to in self._connections

    def __repr__(self):
        return '<Comm %s>' % (self.identity, )


def _make_addr(identity):
    return 'tcp://%s' % identity
