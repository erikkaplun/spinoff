# coding: utf8
from __future__ import print_function, absolute_import

import inspect
import random
import re
import sys
import traceback
from cStringIO import StringIO
from collections import deque
from decimal import Decimal
from pickle import Unpickler, Pickler, BUILD

from twisted import internet
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from txzmq import ZmqEndpoint

from spinoff.actor import Ref, Uri, Node
from spinoff.actor.events import Events, DeadLetter
from spinoff.util.logging import Logging, logstring


dbg = lambda *args, **kwargs: print(file=sys.stderr, *args, **kwargs)


PING = 'ping'
PONG = 'pong'


_dumpmsg = lambda msg: msg[:20] + (msg[20:] and '...')


class ConnectedNode(object):
    def __init__(self, state, last_seen):
        self.state = state
        self.last_seen = last_seen
        self.queue = deque()


class Hub(Logging):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """

    max_silence_between_heartbeats = 5.0
    time_to_keep_hope = 55.0

    queue_item_lifetime = max_silence_between_heartbeats + time_to_keep_hope

    _guardian = None

    def __init__(self, incoming, outgoing, node, reactor=reactor):
        """During testing, the address of this hub/node on the mock network can be the same as its name, for added simplicity
        of not having to have a mapping between logical node names and "physical" mock addresses."""
        assert node

        self.reactor = reactor

        self.outgoing = outgoing
        incoming.gotMessage = self.got_message
        incoming.addEndpoints([ZmqEndpoint('bind', node)])

        self.registry = {}
        self.node = node
        self.connections = {}

        l1 = LoopingCall(self.send_heartbeat)
        l1.clock = reactor
        l1.start(1.0)

        l2 = LoopingCall(self.clean_queue)
        l2.clock = reactor
        l2.start(1.0)

    def get_guardian(self):
        return self._guardian

    def set_guardian(self, guardian):
        if self._guardian:
            raise RuntimeError("Hub already bound to a Guardian")
        self._guardian = guardian

    guardian = property(get_guardian, set_guardian)

    def lookup(self, addr):
        uri = Uri.parse(addr)
        # self.dbg("looked up %r from %s" % (uri, addr))
        if uri.root.node == self.node:
            assert self.guardian
            return self.guardian.lookup(uri)
        return Ref(cell=None, uri=uri, is_local=False, hub=self)

    def register(self, actor):
        # TODO: use weakref
        # self.dbg(actor, '=>', actor.uri.path)
        assert actor.uri.node == self.node
        self.registry[actor.uri.path] = actor

    @logstring(u"⇝")
    def send_message(self, ref, msg, *_, **__):
        # self.dbg(u"%r → %r" % (msg, ref))
        addr = ref.uri.node

        if addr == self.node:
            localref = self.guardian.lookup(ref.uri)
            if not localref:
                raise RuntimeError("Attempt to look up a non-existent local actor %r" % (ref,))
            ref.cell = localref.cell
            ref.is_local = True
            del ref._hub
            ref << msg
            return

        conn = self.connections.get(addr)
        if not conn:
            self.dbg("%s set from not-known => %s" % (addr, 'radiosilence',))
            conn = self.connections[addr] = ConnectedNode(
                state='radiosilence',
                # so last_seen checks would mark the node as `silentlyhoping` in `time_to_keep_hope` seconds from now;
                # conceptually speaking, this means the node seems to have always existed but went out of view exactly
                # now, and before sending can start "again", it needs to come back:
                last_seen=self.reactor.seconds() - self.max_silence_between_heartbeats
            )
            self._connect(addr, conn)

        if conn.state == 'visible':
            self.outgoing.sendMsg((ref.uri.node, self._dumps((ref.uri.path, msg))))
        else:
            if conn.queue is None:
                self.emit_deadletter((ref, msg))
            else:
                conn.queue.append(((ref, msg), self.reactor.seconds()))

    @logstring(u"⇜")
    def got_message(self, sender_addr, msg):
        if msg in (PING, PONG):
            self.dbg(u"❤ %s ← %s" % (msg, sender_addr,))
        else:
            path, msg_ = self._loads(msg)
            self.dbg(u"%r ← %s   → %s" % (msg_, sender_addr, path))
            if path in self.registry:
                self.registry[path].send(msg_)
            else:
                Events.log(DeadLetter(Ref(None, Uri.parse(path)), msg_))

        if sender_addr not in self.connections:
            assert msg == PING, "initial message sent to another node should be PING"
            self.dbg("%s went not-known => %s" % (sender_addr, 'reverse-radiosilence',))
            conn = self.connections[sender_addr] = ConnectedNode(
                state='reverse-radiosilence',
                last_seen=self.reactor.seconds())
            self._connect(sender_addr, conn)
        else:
            conn = self.connections[sender_addr]
            conn.last_seen = self.reactor.seconds()

            prevstate = conn.state
            conn.state = 'reverse-radiosilence' if msg == PING else 'visible'
            if prevstate != conn.state:
                self.dbg("%s went %s => %s" % (sender_addr, prevstate, conn.state))
            if prevstate != 'visible' and conn.state == 'visible':
                while conn.queue:
                    (ref, queued_msg), _ = conn.queue.popleft()
                    assert ref.uri.node == sender_addr
                    self.outgoing.sendMsg((sender_addr, self._dumps((ref.uri.path, queued_msg))))

    def _connect(self, addr, conn):
        # self.dbg("...connecting to %s" % (addr,))
        self.outgoing.addEndpoints([ZmqEndpoint('connect', addr)])
        # send one heartbeat immediately for better latency
        self.dbg(u"►► ❤ → %s" % (addr,))
        self.heartbeat_one(addr, PING if conn.state == 'radiosilence' else PONG)

    @logstring(u"❤")
    def send_heartbeat(self):
        try:
            # self.dbg("→ %r" % (list(self.connections),))
            t = self.reactor.seconds()
            consider_dead_from = t - self.max_silence_between_heartbeats
            consider_lost_from = consider_dead_from - self.time_to_keep_hope
            # self.dbg("consider_dead_from", consider_dead_from, "consider_lost_from", consider_lost_from)

            for addr, conn in self.connections.items():
                # self.dbg("%s last seen at %ss" % (addr, conn.last_seen))
                if conn.state == 'silentlyhoping':
                    # self.dbg("silently hoping...")
                    self.heartbeat_one(addr, PING)
                elif conn.last_seen < consider_lost_from:
                    self.dbg("%s went %s => %s after %ds of silence" % (addr, conn.state, 'silentlyhoping', (t - conn.last_seen)))
                    conn.state = 'silentlyhoping'
                    for msg, _ in conn.queue:
                        # self.dbg("dropping %r" % (msg,))
                        self.emit_deadletter(msg)
                    conn.queue = None
                    self.heartbeat_one(addr, PING)
                elif conn.last_seen < consider_dead_from:
                    if conn.state != 'radiosilence':
                        self.dbg("%s went %s => %s" % (addr, conn.state, 'radiosilence',))
                        conn.state = 'radiosilence'
                    self.heartbeat_one(addr, PING)
                else:
                    # self.dbg("%s still %s; not seen for %s" % (addr, conn.state, '%ds' % (t - conn.last_seen) if conn.last_seen is not None else 'eternity',))
                    self.heartbeat_one(addr, PONG)
            # self.dbg(u"%s ✓" % (self.reactor.seconds(),))
        except Exception:
            self.panic("failed to send heartbeat:\n", traceback.format_exc())

    @logstring(u"⇝ ❤")
    def heartbeat_one(self, addr, signal):
        self.log(u"%s →" % (signal,), addr)
        self.outgoing.sendMsg((addr, signal))

    def clean_queue(self):
        for conn in self.connections.values():
            try:
                keep_until = self.reactor.seconds() - self.queue_item_lifetime
                # self.dbg(conn.queue, "keep_until = %r, queue_item_lifetime = %r" % (keep_until, self.queue_item_lifetime,))
                q = conn.queue
                while q:
                    msg, timestamp = q[0]
                    if timestamp >= keep_until:
                        break
                    else:
                        q.popleft()
                        self.emit_deadletter(msg)
            except Exception:
                self.panic("failed to clean queue:\n", traceback.format_exc())

    def emit_deadletter(self, (ref, msg)):
        # self.dbg(DeadLetter(ref, msg))
        Events.log(DeadLetter(ref, msg))

    def logstate(self):
        return {str(self.reactor.seconds()): True}

    def _dumps(self, msg):
        buf = StringIO()
        OutgoingMessagePickler(self, buf).dump(msg)
        return buf.getvalue()

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()

    def __repr__(self):
        return '<%s>' % (self.node,)


class IncomingMessageUnpickler(Unpickler):
    """Unpickler for attaching a `Hub` instance to all deserialized `Ref`s."""

    def __init__(self, dude, file):
        Unpickler.__init__(self, file)
        self.dude = dude

    # called by `Unpickler.load` before an uninitalized object is about to be filled with members;
    def _load_build(self):
        """See `pickle.py` in Python's source code."""
        # if the ctor. function (penultimate on the stack) is the `Ref` class...
        if isinstance(self.stack[-2], Ref):
            state = self.stack[-1]
            # Ref.__setstate__ will know it's a remote ref if the state is a tuple
            self.stack[-1] = (state, self.dude)
            self.load_build()  # continue with the default implementation

            # detect our own refs sent back to us
            ref = self.stack[-1]
            if ref.uri.node == self.dude.node:
                ref.is_local = True
                del ref._hub
        else:
            self.load_build()

    dispatch = dict(Unpickler.dispatch)  # make a copy of the original
    dispatch[BUILD] = _load_build  # override the handler of the `BUILD` instruction


class OutgoingMessagePickler(Pickler):
    def __init__(self, hub, *args, **kwargs):
        Pickler.__init__(self, protocol=2, *args, **kwargs)
        self.hub = hub

    def save_ref(self, ref):
        if ref.uri.node == self.hub.node:
            # we're probably being serialized for wire-transfer, so for the deserialized dopplegangers of us on other nodes
            # to be able to deliver to us messages, we need to register ourselves with the hub we belong to; if we're being
            # serialized for other reasons (such as storing to disk), it won't harm us, especially when `Hub.registry` is
            # made to use weakrefs.
            self.hub.register(ref)
        self.save_reduce(Ref, (None, None), ref.__getstate__(), obj=ref)
    dispatch = dict(Pickler.dispatch)
    dispatch[Ref] = save_ref


class MockNetwork(Logging):
    """Represents a mock network with only ZeroMQ ROUTER and DEALER sockets on it."""

    def __init__(self, clock):
        self.listeners = {}
        self.queue = []
        self.test_context = inspect.stack()[1][3]
        self.connections = set()
        self.clock = clock

        self._packet_loss = {}

    def node(self, addr, nodeid=None):
        """Creates a new node with the specified name, with `MockSocket` instances as incoming and outgoing sockets.

        Returns the implementation object created for the node from the cls, args and address specified, and the sockets.
        `cls` must be a callable that takes the insock and outsock, and the specified args and kwargs.

        """
        self._validate_addr(addr)
        insock = MockInSocket(addEndpoints=lambda endpoints: self.bind(addr, insock, endpoints))
        outsock = MockOutSocket(addEndpoints=lambda endpoints: self.connect(addr, endpoints),
                                sendMsg=lambda msg: self.enqueue(addr, msg))

        return Node(hub=Hub(insock, outsock, node=nodeid or addr, reactor=self.clock))

    # def mapperdaemon(self, addr):
    #     pass

    def packet_loss(self, percent, src, dst):
        self._packet_loss[(src, dst)] = percent / 100.0

    def _validate_addr(self, addr):
        assert re.match('.+:[0-9]+', addr), "addresses should be in the format <ip-or-hostname>:<port>"

    def bind(self, addr, sock, endpoints):
        assert all(x.type == 'bind' for x in endpoints), "Hubs should only bind in-sockets and never connect"
        assert len(endpoints) == 1, "Hubs should only bind in-sockets to a single network address"
        endpoint, = endpoints
        assert endpoint.address == addr, "Hubs should only bind its in-socket to the address given to the Hub"
        if addr in self.listeners:
            raise TypeError("addr %r already registered on the network" % (addr,))
        self.listeners[addr] = sock

    def connect(self, addr, endpoints):
        for endpoint in endpoints:
            assert endpoint.type == 'connect', "Hubs should only connect MockOutSockets and not bind"
            self._validate_addr(endpoint.address)
            assert (addr, endpoint.address) not in self.connections
            self.dbg(u"%s → %s" % (addr, endpoint.address))
            self.connections.add((addr, endpoint.address))

    @logstring(u"⇝")
    def enqueue(self, src, (dst, msg)):
        assert isinstance(msg, bytes), "Message payloads sent out by Hub should be bytes"
        assert (src, dst) in self.connections, "Hubs should only send messages to addresses they have previously connected to"

        # self.dbg(u"%r → %s" % (_dumpmsg(msg), dst))
        self.queue.append((src, dst, msg))

    @logstring(u"↺")
    def transmit(self):
        """Puts all currently pending sent messages to the insock buffer of the recipient of the message.

        This is more useful than immediate "delivery" because it allows full flexibility of the order in which tests
        set up nodes and mock actors on those nodes, and of the order in which messages are sent out from a node.

        """
        if not self.queue:
            return

        deliverable = []

        for src, dst, msg in self.queue:
            # assert (src, dst) in self.connections, "Hubs should only send messages to addresses they have previously connected to"

            if random.random() <= self._packet_loss.get((src, dst), 0.0):
                self.dbg("packet lost: %r  %s → %s" % (msg, src, dst))
                continue

            if dst not in self.listeners:
                pass  # self.dbg(u"%r ⇝ ↴" % (_dumpmsg(msg),))
            else:
                # self.dbg(u"%r → %s" % (_dumpmsg(msg), dst))
                sock = self.listeners[dst]
                deliverable.append((msg, src, sock))

        del self.queue[:]

        for msg, src, sock in deliverable:
            sock.gotMessage(src, msg)

    def simulate(self, duration, step=0.1):
        MAX_PRECISION = 5
        step = round(Decimal(step), MAX_PRECISION)
        if not step:
            raise TypeError("step value to simulate must be positive and with a precision of less than or equal to %d "
                            "significant figures" % (MAX_PRECISION,))
        time_left = duration
        while True:
            # self.dbg("@ %rs" % (duration - time_left,))
            self.transmit()
            self.clock.advance(step)
            if time_left <= 0:
                break
            else:
                time_left -= step

    def logstate(self):
        return {str(internet.reactor.seconds()): True}

    def __repr__(self):
        return 'network'


class MockInSocket(object):
    """A fake (ZeroMQ-DEALER-like) socket.

    This will instead be a ZeroMQ DEALER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, addEndpoints):
        self.addEndpoints = addEndpoints

    def gotMessage(self, msg):
        assert False, "Hub should define gotMessage on the incoming transport"


class MockOutSocket(object):
    """A fake (ZeroMQ-ROUTER-like) socket.

    This will instead be a ZeroMQ ROUTER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, sendMsg, addEndpoints):
        self.sendMsg = sendMsg
        self.addEndpoints = addEndpoints


class MagicRegistry(object):
    def __init__(self):
        self.actors = {}
        self.created_actors = []

    def __getitem__(self, path):
        if path not in self.actors:
            actor = self.actors[path] = MagicActor(path)
            self.created_actors.append(actor)
        return self.actors[path]


class MagicActor(object):
    def __init__(self, path):
        self.path = path
        self.messages = []
        self.send = self.messages.append
