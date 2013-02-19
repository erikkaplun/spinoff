# coding: utf8
from __future__ import print_function, absolute_import

import random
from decimal import Decimal

from zope.interface import implements
from zope.interface.verify import verifyClass

from spinoff.actor import Node
from spinoff.util.logging import logstring, dbg
from spinoff.remoting.hub import IHub
from spinoff.remoting.validation import _assert_valid_nodeid, _assert_valid_addr


class Hub(object):
    implements(IHub)

    def send_message(self, nid, msg_h):
        pass
# verifyClass(IHub, Hub)


class MockNetwork(object):  # pragma: no cover
    def __init__(self, clock):
        self.listeners = {}
        self.queue = []
        self.connections = set()
        self.clock = clock

        self._packet_loss = {}

    def node(self, nodeid):
        """Creates a new node with the specified name, with `MockSocket` instances as incoming and outgoing sockets.

        Returns the implementation object created for the node from the cls, args and address specified, and the sockets.
        `cls` must be a callable that takes the insock and outsock, and the specified args and kwargs.

        """
        _assert_valid_nodeid(nodeid)
        # addr = 'tcp://' + nodeid
        # insock = MockInSocket(addEndpoints=lambda endpoints: self.bind(addr, insock, endpoints))
        # outsock = lambda: MockOutSocket(addr, self)

        return Node(hub=Hub(nodeid=nodeid))

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

    def disconnect(self, src, dst):
        assert (src, dst) in self.connections, "Outgoing sockets should only disconnect from addresses they have previously connected to"
        self.connections.remove((src, dst))

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
    def simulate(self, duration, step=Decimal('0.1')):
        MAX_PRECISION = 5
        step = round(Decimal(str(step)) if not isinstance(step, Decimal) else step, MAX_PRECISION)
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
        return {str(self.clock.seconds()): True}

    def __repr__(self):
        return 'mock-network'
