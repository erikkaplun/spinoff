# coding: utf8
from __future__ import print_function, absolute_import

import inspect
import random
import re
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

from spinoff.actor import _actor
from spinoff.actor import Ref, Uri, Node
from spinoff.actor._actor import _VALID_NODEID_RE, _validate_nodeid
from spinoff.actor.events import Events, DeadLetter
from spinoff.util.logging import Logging, logstring
from spinoff.util.pattern_matching import ANY


# TODO: use shorter messages outside of testing
PING = 'ping'
PONG = 'pong'


_VALID_ADDR_RE = re.compile('tcp://%s' % (_VALID_NODEID_RE.pattern,))


class ConnectedNode(object):
    def __init__(self, state, last_seen):
        self.state = state
        self.last_seen = last_seen
        self.queue = deque()


class Hub(Logging):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """
    __doc_HEARTBEAT_INTERVAL__ = (
        "Time on seconds after which to send out a heartbeat signal to all known nodes. Regular messages can be "
        "subsituted by the framework for heartbeats to save network bandwidth.")
    HEARTBEAT_INTERVAL = 1.0

    __doc_MAX_SILENCE_BETWEEN_HEARTBEATS__ = (
        "Maximum length of silence in seconds between two consecutive heartbeat signals from a node after which to "
        "consider the node as temporarily not available, put it in the 'radio-silence' state, and start queueing all "
        "messages posted to it by actors.")
    MAX_SILENCE_BETWEEN_HEARTBEATS = 5.0

    __doc_TIME_TO_KEEP_HOPE__ = (
        "Time in seconds after which the 'radio-silence' state transforms into a 'silently-hoping' state wherein the "
        "is considered to have gone offline for an extended duration. All currently queued and any future messages to"
        "that node will immediately be turned into `DeadLetter` events.")
    TIME_TO_KEEP_HOPE = 55.0

    __doc_QUEUE_ITEM_LIFETIME__ = (
        "Time in seconds for which to keep a queued message alive, after which the message is turned into a "
        "`DeadLetter` event. The default value is chosen such that exactly when the target node goes into the "
        "'silently-hoping' visibility state, all messages to it are discarded.")
    QUEUE_ITEM_LIFETIME = MAX_SILENCE_BETWEEN_HEARTBEATS + TIME_TO_KEEP_HOPE

    node = None

    def __init__(self, incoming, outgoing, node, reactor=reactor):
        if not node or not isinstance(node, str):  # pragma: no cover
            raise TypeError("The 'node' argument to Hub must be a str")
        _validate_nodeid(node)
        self.node = node
        self.addr = 'tcp://' + node if node else None

        self.reactor = reactor

        self.outgoing = outgoing
        incoming.gotMessage = self._got_message
        incoming.addEndpoints([ZmqEndpoint('bind', self.addr)])

        self.connections = {}

        l1 = LoopingCall(self._manage_heartbeat_and_visibility)
        l1.clock = reactor
        l1.start(self.HEARTBEAT_INTERVAL)

        l2 = LoopingCall(self._purge_old_items_in_queue)
        l2.clock = reactor
        l2.start(1.0)

    _guardian = None

    def logstate(self):  # pragma: no cover
        return {str(self.reactor.seconds()): True}

    def _get_guardian(self):
        return self._guardian

    def _set_guardian(self, guardian):
        if self._guardian:  # pragma: no cover
            raise RuntimeError("Hub already bound to a Guardian")
        self._guardian = guardian

    guardian = property(_get_guardian, _set_guardian)

    @logstring(u"⇝")
    def send(self, msg, to_remote_actor_pointed_to_by):
        ref = to_remote_actor_pointed_to_by
        # self.dbg(u"%r → %r" % (msg, ref))

        nodeid = ref.uri.node

        # assert addr and addr != self.node, "TODO: remote-ref pointing to the local node detected"
        # TODO: if it's determined that it makes sense to allow the existence of such refs, enable the following block
        if not nodeid or nodeid == self.node:
            cell = self.guardian.lookup_cell(ref.uri)
            if cell:
                ref._cell = cell
                ref.is_local = True
                ref << msg
            else:
                ref.is_local = True  # next time, just put it straight to DeadLetters
                Events.log(DeadLetter(ref, msg))
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
                Events.log(DeadLetter(ref, msg))
            else:
                conn.queue.append(((ref, msg), self.reactor.seconds()))

    @logstring(u"⇜")
    def _got_message(self, sender_addr, msg):
        if msg in (PING, PONG):
            self.dbg(u"❤ %s ← %s" % (msg, sender_addr,))
        else:
            path, msg_ = self._loads(msg)
            self.dbg(u"%r ← %s   → %s" % (msg_, sender_addr, path))
            cell = self.guardian.lookup_cell(Uri.parse(path))
            if not cell:
                if ('_watched', ANY) == msg_:
                    watched_ref = Ref(cell=None, is_local=False, uri=Uri.parse(self.node + path))
                    _, watcher = msg_
                    self.dbg("%r which does not exist watched by %r" % (watched_ref, watcher))
                    watcher << ('terminated', watched_ref)
                else:
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
        self._heartbeat_once(addr, PING if conn.state == 'radiosilence' else PONG)

    @logstring(u"❤")
    def _manage_heartbeat_and_visibility(self):
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
                    self._heartbeat_once(addr, PING)
                elif conn.last_seen < consider_lost_from:
                    self.dbg("%s went %s => %s after %ds of silence" % (addr, conn.state, 'silentlyhoping', (t - conn.last_seen)))
                    conn.state = 'silentlyhoping'
                    for (ref, msg), _ in conn.queue:
                        # self.dbg("dropping %r" % (msg,))
                        Events.log(DeadLetter(ref, msg))
                    conn.queue = None
                    self._heartbeat_once(addr, PING)
                elif conn.last_seen < consider_dead_from:
                    if conn.state != 'radiosilence':
                        self.dbg("%s went %s => %s" % (addr, conn.state, 'radiosilence',))
                        conn.state = 'radiosilence'
                    self._heartbeat_once(addr, PING)
                else:
                    # self.dbg("%s still %s; not seen for %s" % (addr, conn.state, '%ds' % (t - conn.last_seen) if conn.last_seen is not None else 'eternity',))
                    self._heartbeat_once(addr, PONG)
            # self.dbg(u"%s ✓" % (self.reactor.seconds(),))
        except Exception:  # pragma: no cover
            self.panic("heartbeat logic failed:\n", traceback.format_exc())

    @logstring(u"⇝ ❤")
    def _heartbeat_once(self, addr, signal):
        assert _valid_addr(addr)
        self.log(u"%s →" % (signal,), addr)
        self.outgoing.sendMsg((addr, signal))

    @logstring("PURGE")
    def _purge_old_items_in_queue(self):
        for conn in self.connections.values():
            try:
                keep_until = self.reactor.seconds() - self.QUEUE_ITEM_LIFETIME
                # self.dbg(conn.queue, "keep_until = %r, QUEUE_ITEM_LIFETIME = %r" % (keep_until, self.QUEUE_ITEM_LIFETIME,))
                q = conn.queue
                while q:
                    (ref, msg), timestamp = q[0]
                    if timestamp >= keep_until:
                        break
                    else:
                        q.popleft()
                        Events.log(DeadLetter(ref, msg))
            except Exception:  # pragma: no cover
                self.panic("failed to clean queue:\n", traceback.format_exc())

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()

    def __repr__(self):
        return '<%s>' % (self.node,)


class HubWithNoRemoting(object):
    """A dummy hub used during networkless testing and in production when no remoting should be available.

    All it does is imitate the interface of the real `Hub`, and report attempts to send remote messages as
    `RuntimeError`s.

    """
    # to be compatible with Hub:
    guardian = None
    node = None

    def send(self, ref, msg):  # pragma: no cover
        raise RuntimeError("Attempt to send a message to a remote ref but remoting is not available")


class IncomingMessageUnpickler(Unpickler, Logging):
    """Unpickler for attaching a `Hub` instance to all deserialized `Ref`s."""

    def __init__(self, hub, file):
        Unpickler.__init__(self, file)
        self.hub = hub

    # called by `Unpickler.load` before an uninitalized object is about to be filled with members;
    def _load_build(self):
        """See `pickle.py` in Python's source code."""
        # if the ctor. function (penultimate on the stack) is the `Ref` class...
        if isinstance(self.stack[-2], Ref):
            if _actor.TESTING:
                state = self.stack[-1]
                # Ref.__setstate__ will know it's a remote ref if the state is a tuple
                self.stack[-1] = (state, self.hub)
                self.load_build()  # continue with the default implementation

            # detect our own refs sent back to us
            ref = self.stack[-1]
            if ref.uri.node == self.hub.node:
                ref.is_local = True
                ref._cell = self.hub.guardian.lookup_cell(ref.uri)
                # self.dbg(("dead " if not ref._cell else "") + "local ref detected")
                if _actor.TESTING:
                    del ref._hub
        else:  # pragma: no cover
            self.load_build()

    dispatch = dict(Unpickler.dispatch)  # make a copy of the original
    dispatch[BUILD] = _load_build  # override the handler of the `BUILD` instruction


def _validate_addr(addr):
    # call from app code
    m = _VALID_ADDR_RE.match(addr)
    if not m:  # pragma: no cover
        raise ValueError("Addresses should be in the format 'tcp://<ip-or-hostname>:<port>': %s" % (addr,))
    port = int(m.group(1))
    if not (0 <= port <= 65535):  # pragma: no cover
        raise ValueError("Ports should be in the range 0-65535: %d" % (port,))


def _assert_valid_nodeid(nodeid):  # pragma: no cover
    try:
        _validate_nodeid(nodeid)
    except ValueError as e:
        raise AssertionError(e.message)


def _assert_valid_addr(addr):  # pragma: no cover
    try:
        _validate_addr(addr)
    except ValueError as e:
        raise AssertionError(e.message)


# semantic alias for prefixing with `assert`
def _valid_addr(addr):  # pragma: no cover
    _assert_valid_addr(addr)
    return True


class MockNetwork(Logging):  # pragma: no cover
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


class MockInSocket(object):  # pragma: no cover
    """A fake (ZeroMQ-DEALER-like) socket.

    This will instead be a ZeroMQ DEALER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, addEndpoints):
        self.addEndpoints = addEndpoints

    def gotMessage(self, msg):
        assert False, "Hub should define gotMessage on the incoming transport"


class MockOutSocket(object):  # pragma: no cover
    """A fake (ZeroMQ-ROUTER-like) socket.

    This will instead be a ZeroMQ ROUTER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, sendMsg, addEndpoints):
        self.sendMsg = sendMsg
        self.addEndpoints = addEndpoints


_dumpmsg = lambda msg: msg[:20] + (msg[20:] and '...')  # pragma: no cover
