# coding: utf8
from __future__ import print_function, absolute_import

import struct
import traceback
from cStringIO import StringIO

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from txzmq import ZmqEndpoint, ZmqFactory, ZmqPushConnection, ZmqPullConnection

from spinoff.actor import Ref, Uri
from spinoff.actor.events import Events, DeadLetter, RemoteDeadLetter
from spinoff.actor.resolv import resolve
from spinoff.util.logging import logstring, panic
from spinoff.util.pattern_matching import ANY
from .connection import Connection
from .pickler import IncomingMessageUnpickler
from .validation import _validate_nodeid, _valid_addr, _PROTO_ADDR_RE

# TODO: use shorter messages outside of testing
# MUST be a single char
PING = b'0'
DISCONNECT = b'1'

PING_VERSION_FORMAT = '!I'


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """
    __doc_HEARTBEAT_INTERVAL__ = (
        "Time on seconds after which to send out a heartbeat signal to all known nodes. Regular messages can be "
        "subsituted by the framework for heartbeats to save network bandwidth.")
    HEARTBEAT_INTERVAL = 1.0
    ALLOWED_HEARTBEAT_DELAY = HEARTBEAT_INTERVAL * 0.2

    HEARTBEAT_MAX_SILENCE = 15.0

    nodeid = None

    def __init__(self, nodeid, reactor=reactor):
        if not nodeid or not isinstance(nodeid, str):  # pragma: no cover
            raise TypeError("The 'nodeid' argument to Hub must be a str")
        _validate_nodeid(nodeid)

        self.reactor = reactor

        self.nodeid = nodeid

        self.addr = 'tcp://' + nodeid if nodeid else None

        # guarantees that terminations will not go unnoticed and that termination messages won't arrive out of order
        self.version = 0

        zf = ZmqFactory()

        self.insock = ZmqPullConnection(zf)
        self.insock.gotMultipart = self._got_message
        self.insock.addEndpoints([ZmqEndpoint('bind', _resolve_addr(self.addr))])

        self.outsock_factory = lambda: ZmqPushConnection(zf, linger=0)
        self.connections = {}

        self._next_heartbeat = reactor.callLater(self.HEARTBEAT_INTERVAL, self._manage_heartbeat_and_visibility)
        self._next_heartbeat_t = reactor.seconds() + self.HEARTBEAT_INTERVAL

    @logstring(u"⇜")
    def _got_message(self, (sender_addr, msg)):
        conn = self.connections.get(sender_addr)
        t = self.reactor.seconds()
        if t > self._next_heartbeat_t + self.ALLOWED_HEARTBEAT_DELAY / 2.0:
            self._next_heartbeat.cancel()
            self._manage_heartbeat_and_visibility()

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

        elif msg == DISCONNECT:
            if conn:
                conn.close(disconnect_other=False)
                del self.connections[sender_addr]

        else:
            path, msg = self._loads(msg)
            if conn:
                conn.seen = t
                self._deliver_local(path, msg, sender_addr)
            else:
                self._connect(sender_addr)
                self._remote_dead_letter(path, msg, sender_addr)

    @logstring(u"❤")
    def _manage_heartbeat_and_visibility(self):
        t = self.reactor.seconds()
        self._next_heartbeat_t = t + self.HEARTBEAT_INTERVAL
        try:
            t_gone = t - self.HEARTBEAT_MAX_SILENCE
            for addr, conn in self.connections.items():
                if conn.seen < t_gone:
                    conn.close()
                    del self.connections[addr]
                else:
                    conn.heartbeat()
        except Exception:  # pragma: no cover
            panic("heartbeat logic failed:\n", traceback.format_exc())
        finally:
            self._next_heartbeat = self.reactor.callLater(self.HEARTBEAT_INTERVAL, self._manage_heartbeat_and_visibility)

    def _connect(self, addr):
        assert _valid_addr(addr)
        conn = self.connections[addr] = Connection(
            owner=self,
            addr=_resolve_addr(addr),
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

        t = self.reactor.seconds()
        if t > self._next_heartbeat_t + self.ALLOWED_HEARTBEAT_DELAY / 2.0:
            if self._next_heartbeat:
                self._next_heartbeat.cancel()
                self._manage_heartbeat_and_visibility()

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

    @inlineCallbacks
    def stop(self):
        self._next_heartbeat.cancel()
        self._next_heartbeat = None
        self.insock.shutdown()
        for _, conn in self.connections.items():
            yield conn.close()

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()

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


_dumpmsg = lambda msg: msg[:20] + (msg[20:] and '...')  # pragma: no cover


def _resolve_addr(addr):
    m = _PROTO_ADDR_RE.match(addr)
    proto, nodeid = m.groups()[0:2]

    if nodeid:
        host, port = nodeid.split(':')
        return '%s%s:%s' % (proto, resolve(host), port)
    return addr
