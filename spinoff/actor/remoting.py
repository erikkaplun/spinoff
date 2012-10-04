# coding: utf8
from __future__ import print_function, absolute_import

import inspect
import random
import re
import struct
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
from spinoff.actor.events import Events, DeadLetter, RemoteDeadLetter
from spinoff.util.logging import logstring, dbg, log, panic
from spinoff.util.pattern_matching import ANY, IN


# TODO: use shorter messages outside of testing
# MUST be a single char
PING = b'0'

PING_VERSION_FORMAT = '!I'

_VALID_ADDR_RE = re.compile('tcp://%s' % (_VALID_NODEID_RE.pattern,))


class Connection(object):
    watching_actors = None
    queue = None

    def __init__(self, owner, addr, sock, our_addr, time, known_remote_version):
        self.owner = owner
        self.addr = addr
        self.sock = sock = sock
        self.our_addr = our_addr
        # even if this is a fresh connection, there's no need to set seen to None--in fact it would be illogical, as by
        # connecting, we're making an assumption that it exists; and pragmatically speaking, the heartbeat algorithm
        # also wouldn't be able to deal with seen=None without adding an extra attribute.
        self.seen = time

        self.watched_actors = set()
        self.queue = deque()

        self.known_remote_version = known_remote_version

        sock.addEndpoints([ZmqEndpoint('connect', addr)])

    @property
    def is_active(self):
        return self.queue is None

    def send(self, ref, msg):
        if self.queue is not None:
            self.queue.append((ref, msg))
        else:
            self._do_send(dumps((ref.uri.path, msg), protocol=2))

    def established(self, remote_version):
        log()
        self.known_remote_version = remote_version
        self._flush_queue()
        del self.queue

    def close(self):
        log()
        self._kill_queue()
        self._emit_termination_messages()
        self.sock.shutdown()
        del self.sock, self.owner

    @logstring(u" ❤⇝")
    def heartbeat(self):
        self._do_send(PING + struct.pack(PING_VERSION_FORMAT, self.owner.version))
        self.owner.version += 1

    @logstring(u"⇝")
    def _do_send(self, msg):
        self.sock.sendMultipart((self.our_addr, msg))

    def _flush_queue(self):
        q, self.queue = self.queue, None
        while q:
            ref, msg = q.popleft()
            assert ref.uri.root.url == self.addr
            self._do_send(dumps((ref.uri.path, msg), protocol=2))

    def _kill_queue(self):
        q, self.queue = self.queue, None
        while q:
            ref, msg = q.popleft()
            if (IN(['_watched', '_unwatched', 'terminated']), ANY) != msg:
                Events.log(DeadLetter(ref, msg))

    def watch(self, report_to):
        if not self.watching_actors:
            self.watching_actors = set()
        self.watching_actors.add(report_to)

    def unwatch(self, report_to):
        if self.watching_actors:
            self.watching_actors.discard(report_to)

    def _emit_termination_messages(self):
        if self.watching_actors:
            w, self.watching_actors = self.watching_actors, None
            for report_to in w:
                report_to << ('_node_down', self.addr[len('tcp://'):])

    def __del__(self):
        if hasattr(self, 'sock') and self.sock:
            self.sock.shutdown()
            del self.sock

    def __repr__(self):
        return '<connection:%s->%s>' % (self.owner.nodeid, self.addr[len('tcp://'):])


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """
    __doc_HEARTBEAT_INTERVAL__ = (
        "Time on seconds after which to send out a heartbeat signal to all known nodes. Regular messages can be "
        "subsituted by the framework for heartbeats to save network bandwidth.")
    HEARTBEAT_INTERVAL = 1.0

    HEARTBEAT_MAX_SILENCE = 4.0

    nodeid = None

    def __init__(self, insock, outsock_factory, nodeid, reactor=reactor):
        if not nodeid or not isinstance(nodeid, str):  # pragma: no cover
            raise TypeError("The 'nodeid' argument to Hub must be a str")
        _validate_nodeid(nodeid)

        self.reactor = reactor

        self.nodeid = nodeid
        self.addr = 'tcp://' + nodeid if nodeid else None

        # guarantees that terminations will not go unnoticed and that termination messages won't arrive out of order
        self.version = 0

        self.insock = insock
        insock.gotMultipart = self._got_message
        insock.addEndpoints([ZmqEndpoint('bind', self.addr)])

        self.outsock_factory = outsock_factory
        self.connections = {}

        l1 = self._heartbeater = LoopingCall(self._manage_heartbeat_and_visibility)
        l1.clock = reactor
        l1.start(self.HEARTBEAT_INTERVAL)

    @logstring(u"⇜")
    def _got_message(self, (sender_addr, msg)):
        conn = self.connections.get(sender_addr)

        if msg[0] == PING:
            remote_version, = struct.unpack(PING_VERSION_FORMAT, msg[1:])  # not sure this is even necessary

            if not conn:
                self._connect(sender_addr)
            else:
                # first ping arrived:
                if not conn.is_active:
                    # mark connection as established (flushes the queue and starts sending):
                    conn.established(remote_version)
                # version mismatch:
                elif not (remote_version > conn.known_remote_version):
                    # he has restarted. notify our actors of it:
                    conn._emit_termination_messages()

                conn.known_remote_version = remote_version
                conn.seen = self.reactor.seconds()

        else:
            path, msg = self._loads(msg)
            if conn:
                conn.seen = self.reactor.seconds()
                self._deliver_local(path, msg, sender_addr)
            else:
                self._connect(sender_addr)
                self._remote_dead_letter(path, msg, sender_addr)

    @logstring(u"❤")
    def _manage_heartbeat_and_visibility(self):
        try:
            t_gone = self.reactor.seconds() - self.HEARTBEAT_MAX_SILENCE
            for addr, conn in self.connections.items():
                if conn.seen < t_gone:
                    conn.close()
                    del self.connections[addr]
                else:
                    conn.heartbeat()
        except Exception:  # pragma: no cover
            panic("heartbeat logic failed:\n", traceback.format_exc())

    def _connect(self, addr):
        assert _valid_addr(addr)
        conn = self.connections[addr] = Connection(
            owner=self,
            addr=addr,
            sock=self.outsock_factory(),
            our_addr=self.addr,
            time=self.reactor.seconds(),
            known_remote_version=None,
        )

        # dbg(u"►► ❤ →", addr)
        conn.heartbeat()
        return conn

    @logstring(u"⇝")
    def send(self, msg, to_remote_actor_pointed_to_by):
        ref = to_remote_actor_pointed_to_by
        # dbg(u"%r → %r" % (msg, ref))

        nodeid = ref.uri.node

        if nodeid and nodeid != self.nodeid:
            addr = ref.uri.root.url
            conn = self.connections.get(addr)
            if not conn:
                conn = self._connect(addr)
            conn.send(ref, msg)
        else:
            self._send_local(msg, ref)

    def watch_node(self, nodeid, report_to):
        node_addr = 'tcp://' + nodeid
        conn = self.connections.get(node_addr)
        if not conn:
            conn = self._connect(node_addr)
        conn.watch(report_to)

    def unwatch_node(self, nodeid, report_to):
        node_addr = 'tcp://' + nodeid
        conn = self.connections.get(node_addr)
        if conn:
            conn.unwatch(report_to)

    def _send_local(self, msg, ref):
        cell = self.guardian.lookup_cell(ref.uri)
        if cell:
            ref._cell = cell
            ref.is_local = True
            ref << msg
        else:
            ref.is_local = True  # next time, just put it straight to DeadLetters
            if msg not in (('terminated', ANY), ('_watched', ANY), ('_unwatched', ANY)):
                Events.log(DeadLetter(ref, msg))

    def _deliver_local(self, path, msg, sender_addr):
        cell = self.guardian.lookup_cell(Uri.parse(path))
        if not cell:
            if ('_watched', ANY) == msg:
                watched_ref = Ref(cell=None, is_local=True, uri=Uri.parse(self.nodeid + path))
                _, watcher = msg
                watcher << ('terminated', watched_ref)
            else:
                if msg not in (('terminated', ANY), ('_watched', ANY), ('_unwatched', ANY)):
                    self._remote_dead_letter(path, msg, sender_addr)
        else:
            cell.receive(msg)  # XXX: force_async=True perhaps?

    def stop(self):
        self._heartbeater.stop()
        self.insock.shutdown()
        for _, conn in self.connections.items():
            conn.close()

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()

    def logstate(self):  # pragma: no cover
        return {str(self.reactor.seconds()): True}

    def _remote_dead_letter(self, path, msg, from_):
        uri = Uri.parse(self.nodeid + path)
        ref = Ref(cell=self.guardian.lookup_cell(uri), uri=uri, is_local=True)
        Events.log(RemoteDeadLetter(ref, msg, from_))

    def _get_guardian(self):
        return self._guardian

    def _set_guardian(self, guardian):
        if self._guardian:  # pragma: no cover
            raise RuntimeError("Hub already bound to a Guardian")
        self._guardian = guardian
    _guardian = None
    guardian = property(_get_guardian, _set_guardian)

    def __repr__(self):
        return '<remoting:%s>' % (self.nodeid,)


class HubWithNoRemoting(object):
    """A dummy hub used during networkless testing and in production when no remoting should be available.

    All it does is imitate the interface of the real `Hub`, and report attempts to send remote messages as
    `RuntimeError`s.

    """
    # to be compatible with Hub:
    guardian = None
    nodeid = None

    def send(self, *args, **kwargs):  # pragma: no cover
        raise RuntimeError("Attempt to send a message to a remote ref but remoting is not available")

    def stop(self):  # pragma: no cover
        pass

    def watch_node(self, *args, **kwargs):
        raise RuntimeError("Attempt to watch a remote node but remoting is not available")

    def unwatch_node(self, *args, **kwargs):
        raise RuntimeError("Attempt to unwatch a remote node but remoting is not available")


class IncomingMessageUnpickler(Unpickler):
    """Unpickler for attaching a `Hub` instance to all deserialized `Ref`s."""

    def __init__(self, hub, file):
        Unpickler.__init__(self, file)
        self.hub = hub

    # called by `Unpickler.load` before an uninitalized object is about to be filled with members;
    def _load_build(self):
        """See `pickle.py` in Python's source code."""
        # if the ctor. function (penultimate on the stack) is the `Ref` class...
        if isinstance(self.stack[-2], Ref):
            # Ref.__setstate__ will know it's a remote ref if the state is a tuple
            self.stack[-1] = (self.stack[-1],
                              self.hub if _actor.TESTING else None)

            self.load_build()  # continue with the default implementation

            # detect our own refs sent back to us
            ref = self.stack[-1]
            if ref.uri.node == self.hub.nodeid:
                ref.is_local = True
                ref._cell = self.hub.guardian.lookup_cell(ref.uri)
                # dbg(("dead " if not ref._cell else "") + "local ref detected")
                del ref._hub  # local refs never need hubs
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


class MockNetwork(object):  # pragma: no cover
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
        outsock = lambda: MockOutSocket(addr, self)

        return Node(hub=Hub(insock, outsock, nodeid=nodeid, reactor=self.clock))

    def outsock_addEndpoints(self, src, endpoints):
        self.connect(src, endpoints)

    def outsock_sendMultipart(self, src, dst, msgParts):
        self.enqueue(src, dst, msgParts)

    def outsock_shutdown(self, addr):
        self.disconnect(addr)

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
            dbg(u"%s → %s" % (addr, endpoint.address))
            self.connections.add((addr, endpoint.address))

    def disconnect(self, addr):
        found = [(src, dst) for src, dst in self.connections if addr == src]
        assert found, "Outgoing sockets should only disconnect from addresses they have previously connected to"
        self.connections.remove(found[0])

    @logstring(u"⇝")
    def enqueue(self, src, dst, msg):
        _assert_valid_addr(src)
        _assert_valid_addr(dst)
        assert isinstance(msg, tuple) and all(isinstance(x, bytes) for x in msg), "Message payloads sent out by Hub should be tuples containing bytse"
        assert (src, dst) in self.connections, "Hubs should only send messages to addresses they have previously connected to"

        # dbg(u"%r → %s" % (_dumpmsg(msg), dst))
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
                dbg("packet lost: %r  %s → %s" % (msg, src, dst))
                continue

            if dst not in self.listeners:
                pass  # dbg(u"%r ⇝ ↴" % (_dumpmsg(msg),))
            else:
                # dbg(u"%r → %s" % (_dumpmsg(msg), dst))
                sock = self.listeners[dst]
                deliverable.append((msg, src, sock))

        del self.queue[:]

        for msg, src, sock in deliverable:
            sock.gotMultipart(msg)

    # XXX: change `duration` into `iterations` so that changing `step` wouldn't affect tests, as far as non-packet-lossy tests are concerned
    def simulate(self, duration, step=0.1):
        MAX_PRECISION = 5
        step = round(Decimal(step), MAX_PRECISION)
        if not step:
            raise TypeError("step value to simulate must be positive and with a precision of less than or equal to %d "
                            "significant figures" % (MAX_PRECISION,))
        time_left = duration
        while True:
            # dbg("@ %rs" % (duration - time_left,))
            self.transmit()
            self.clock.advance(step)
            if time_left <= 0:
                break
            else:
                time_left -= step

    def logstate(self):
        return {str(internet.reactor.seconds()): True}

    def __repr__(self):
        return 'mock-network'


class MockInSocket(object):  # pragma: no cover
    """A fake (ZeroMQ-ROUTER-like) socket that only supports receiving.

    This will instead be a ZeroMQ ROUTER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, addEndpoints):
        self.addEndpoints = addEndpoints

    def gotMultipart(self, msg):
        assert False, "Hub should define gotMultipart on the incoming transport"

    def shutdown(self):
        pass


class MockOutSocket(object):  # pragma: no cover
    """A fake (ZeroMQ-ROUTER-like) socket that only supports sending.

    This will instead be a ZeroMQ ROUTER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, addr, owner):
        self.addr, self.owner = addr, owner
        self.endpoint_addr = None

    def sendMultipart(self, msgParts):
        assert self.endpoint_addr, "Outgoing sockets should not send before they connect"
        self.owner.outsock_sendMultipart(src=self.addr, dst=self.endpoint_addr, msgParts=msgParts)

    def addEndpoints(self, endpoints):
        assert len(endpoints) == 1, "Outgoing sockets should not connect to more than 1 endpoint"
        dbg(endpoints[0].address)
        self.endpoint_addr = endpoints[0].address
        self.owner.outsock_addEndpoints(self.addr, endpoints)

    def shutdown(self):
        dbg()
        self.owner.outsock_shutdown(self.addr)

    def __repr__(self):
        return '<outsock:%s>' % (self.addr,)


_dumpmsg = lambda msg: msg[:20] + (msg[20:] and '...')  # pragma: no cover
