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
from pickle import Unpickler, BUILD
from cPickle import dumps

from twisted import internet
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from txzmq import ZmqEndpoint

from spinoff.actor import Ref, Uri, Node
from spinoff.actor._actor import _VALID_NODEID_RE, _validate_nodeid
from spinoff.actor.events import Events, DeadLetter
from spinoff.util.logging import Logging, logstring


dbg = lambda *args, **kwargs: print(file=sys.stderr, *args, **kwargs)


PING = 'ping'
PONG = 'pong'


_VALID_ADDR_RE = re.compile('tcp://%s' % (_VALID_NODEID_RE.pattern,))


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
    HEARTBEAT_INTERVAL = 1.0

    MAX_SILENCE_BETWEEN_HEARTBEATS = 5.0
    TIME_TO_KEEP_HOPE = 55.0

    QUEUE_ITEM_LIFETIME = MAX_SILENCE_BETWEEN_HEARTBEATS + TIME_TO_KEEP_HOPE

    _guardian = None

    def __init__(self, incoming, outgoing, node, reactor=reactor):
        """During testing, the address of this hub/node on the mock network can be the same as its name, for added simplicity
        of not having to have a mapping between logical node names and "physical" mock addresses."""
        if not node or not isinstance(node, str):
            raise TypeError("The 'node' argument to Hub must be a str")
        _validate_nodeid(node)
        self.node = node
        self.addr = 'tcp://' + node if node else None

        self.reactor = reactor

        self.outgoing = outgoing
        incoming.gotMessage = self.got_message
        incoming.addEndpoints([ZmqEndpoint('bind', self.addr)])

        self.connections = {}

        l1 = LoopingCall(self.send_heartbeat)
        l1.clock = reactor
        l1.start(self.HEARTBEAT_INTERVAL)

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

    @logstring(u"⇝")
    def send(self, msg, to_remote_actor_pointed_to_by):
        ref = to_remote_actor_pointed_to_by
        # self.dbg(u"%r → %r" % (msg, ref))

        nodeid = ref.uri.node

        # assert addr and addr != self.node, "TODO: remote-ref pointing to the local node detected"
        # TODO: if it's determined that it makes sense to allow the existence of such refs, enable the following block
        if not nodeid or nodeid == self.node:
            cell = self.guardian.lookup_cell(ref.uri)
            if not cell:
                ref.is_local = True  # next time, just put it straight to DeadLetters
                Events.log(DeadLetter(ref, msg))
                return
            ref.cell = cell
            ref.is_local = True
            ref << msg
            return

        addr = ref.uri.root.url

        conn = self.connections.get(addr)
        if not conn:
            self.dbg("%s set from not-known => %s" % (addr, 'radiosilence',))
            conn = self.connections[addr] = ConnectedNode(
                state='radiosilence',
                # so last_seen checks would mark the node as `silentlyhoping` in `TIME_TO_KEEP_HOPE` seconds from now;
                # conceptually speaking, this means the node seems to have always existed but went out of view exactly
                # now, and before sending can start "again", it needs to come back:
                last_seen=self.reactor.seconds() - self.MAX_SILENCE_BETWEEN_HEARTBEATS
            )
            self._connect(addr, conn)

        if conn.state == 'visible':
            self.outgoing.sendMsg((addr, dumps((ref.uri.path, msg), protocol=2)))
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
            cell = self.guardian.lookup_cell(Uri.parse(path))
            if not cell:
                Events.log(DeadLetter(Ref(None, Uri.parse(path)), msg_))
            else:
                cell.receive(msg_)  # XXX: force_async=True perhaps?

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
                    assert ref.uri.root.url == sender_addr
                    self.outgoing.sendMsg((sender_addr, dumps((ref.uri.path, queued_msg), protocol=2)))

    def _connect(self, addr, conn):
        assert _valid_addr(addr)
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
            consider_dead_from = t - self.MAX_SILENCE_BETWEEN_HEARTBEATS
            consider_lost_from = consider_dead_from - self.TIME_TO_KEEP_HOPE
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
        assert _valid_addr(addr)
        self.log(u"%s →" % (signal,), addr)
        self.outgoing.sendMsg((addr, signal))

    def clean_queue(self):
        for conn in self.connections.values():
            try:
                keep_until = self.reactor.seconds() - self.QUEUE_ITEM_LIFETIME
                # self.dbg(conn.queue, "keep_until = %r, QUEUE_ITEM_LIFETIME = %r" % (keep_until, self.QUEUE_ITEM_LIFETIME,))
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

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()

    def __repr__(self):
        return '<%s>' % (self.node,)


class HubWithNoRemoting(object):
    # to be compatible with Hub:
    guardian = None
    node = None

    def send(self, ref, msg):
        raise RuntimeError("Attempt to send a message to a remote ref but remoting is not available")


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


def _validate_addr(addr):
    # call from app code
    m = _VALID_ADDR_RE.match(addr)
    if not m:
        raise ValueError("Addresses should be in the format 'tcp://<ip-or-hostname>:<port>': %s" % (addr,))
    port = int(m.group(1))
    if not (0 <= port <= 65535):
        raise ValueError("Ports should be in the range 0-65535: %d" % (port,))


def _assert_valid_nodeid(nodeid):
    try:
        _validate_nodeid(nodeid)
    except ValueError as e:
        raise AssertionError(e.message)


def _assert_valid_addr(addr):
    try:
        _validate_addr(addr)
    except ValueError as e:
        raise AssertionError(e.message)


# semantic alias for prefixing with `assert`
def _valid_addr(addr):
    _assert_valid_addr(addr)
    return True


class MockNetwork(Logging):
    """Represents a mock network with only ZeroMQ ROUTER and DEALER sockets on it."""

    def __init__(self, clock):
        self.listeners = {}
        self.queue = []
        self.test_context = inspect.stack()[1][3]
        self.connections = set()
        self.clock = clock

        self._packet_loss = {}

    def node(self, nodeid):
        """Creates a new node with the specified name, with `MockSocket` instances as incoming and outgoing sockets.

        Returns the implementation object created for the node from the cls, args and address specified, and the sockets.
        `cls` must be a callable that takes the insock and outsock, and the specified args and kwargs.

        """
        _assert_valid_nodeid(nodeid)
        addr = 'tcp://' + nodeid
        insock = MockInSocket(addEndpoints=lambda endpoints: self.bind(addr, insock, endpoints))
        outsock = MockOutSocket(addEndpoints=lambda endpoints: self.connect(addr, endpoints),
                                sendMsg=lambda msg: self.enqueue(addr, msg))

        return Node(hub=Hub(insock, outsock, node=nodeid, reactor=self.clock))

    # def mapperdaemon(self, addr):
    #     pass

    def packet_loss(self, percent, src, dst):
        _assert_valid_addr(src)
        _assert_valid_addr(dst)
        self._packet_loss[(src, dst)] = percent / 100.0

    def bind(self, addr, sock, endpoints):
        assert all(x.type == 'bind' for x in endpoints), "Hubs should only bind in-sockets and never connect"
        assert len(endpoints) == 1, "Hubs should only bind in-sockets to a single network address"
        endpoint, = endpoints
        _assert_valid_addr(addr)
        _assert_valid_addr(endpoint.address)
        assert endpoint.address == addr, "Hubs should only bind its in-socket to the address given to the Hub: %s != %s" % (addr, endpoint.address)
        if addr in self.listeners:
            raise TypeError("addr %r already registered on the network" % (addr,))
        self.listeners[addr] = sock

    def connect(self, addr, endpoints):
        _assert_valid_addr(addr)
        for endpoint in endpoints:
            assert endpoint.type == 'connect', "Hubs should only connect MockOutSockets and not bind"
            _assert_valid_addr(endpoint.address)
            assert (addr, endpoint.address) not in self.connections
            self.dbg(u"%s → %s" % (addr, endpoint.address))
            self.connections.add((addr, endpoint.address))

    @logstring(u"⇝")
    def enqueue(self, src, (dst, msg)):
        _assert_valid_addr(src)
        _assert_valid_addr(dst)
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
            _assert_valid_addr(src)
            _assert_valid_addr(dst)

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
