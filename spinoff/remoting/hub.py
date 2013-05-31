from __future__ import print_function

import time
import socket
import struct

import zmq.green as zmq
from zope.interface import Interface, implements
from zope.interface.verify import verifyClass
from gevent import sleep, spawn, spawn_later
from gevent.socket import gethostbyname
from gevent.lock import RLock

from spinoff.remoting.hublogic import (
    HubLogic, Connect, Disconnect, SigDisconnect, Send, Ping,
    RelaySigNew, RelayConnect, RelaySigConnected, RelaySend, RelayForward, RelaySigNodeDown, RelayNvm,
    Receive, SendFailed, NodeDown, NextBeat, Bind, IN, OUT, flatten)
from spinoff.util.logging import dbg


__all__ = ['Hub']


MSG_HEADER_FORMAT = '!I'
_signals = [struct.pack(MSG_HEADER_FORMAT, x) for x in range(9)]
SIG_DISCONNECT, SIG_NEW_RELAY, SIG_RELAY_CONNECT, SIG_RELAY_CONNECTED, SIG_RELAY_SEND, SIG_RELAY_FORWARDED, SIG_RELAY_NODEDOWN, SIG_RELAY_NVM, SIG_VERIFY_IDENTITY = _signals
MIN_VERSION_VALUE = len(_signals)
MIN_VERSION_BITS = struct.pack(MSG_HEADER_FORMAT, MIN_VERSION_VALUE)


class IHub(Interface):
    def __init__(nid, is_relay=False, on_node_down=None, on_receive=None):
        pass

    def send_message(nid, msg_h):
        pass

    def watch_node(nid, watch_handle):
        pass

    def unwatch_node(nid, watch_handle):
        pass

    def stop():
        pass


_DELETED = object()


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """
    implements(IHub)

    FAKE_INACCESSIBLE_NADDRS = set()

    def __init__(self, nid, is_relay=False, on_node_down=lambda ref, nid: ref << ('_node_down', nid),
                 on_receive=lambda sender_nid, msg_h: print("deliver", msg_h, "from", sender_nid),
                 heartbeat_interval=1.0, heartbeat_max_silence=3.0):
        self.nid = nid
        self.is_relay = is_relay
        self._on_node_down = on_node_down
        self._on_receive = on_receive
        self._lock = RLock()
        self._logic = HubLogic(nid, is_relay=is_relay,
                               heartbeat_interval=heartbeat_interval,
                               heartbeat_max_silence=heartbeat_max_silence)
        self._ctx = zmq.Context()
        self._ctx.linger = 0
        self._insock = self._ctx.socket(zmq.ROUTER)
        self._outsock = self._ctx.socket(zmq.ROUTER)
        self._insock.identity = self._outsock.identity = nid
        self._listener_in = spawn(self._listen, self._insock, IN)
        self._listener_in.link_exception(lambda _: self.stop())
        self._listener_out = spawn(self._listen, self._outsock, OUT)
        self._listener_out.link_exception(lambda _: self.stop())
        self._heartbeater = None
        self._watched_nodes = {}
        self._initialized = True
        self._start()

    def _start(self):
        self._execute(self._logic.start)

    def send_message(self, nid, msg_h):
        self._execute(self._logic.send_message, nid, msg_h, time.time())

    def watch_node(self, nid, watch_handle):
        if nid not in self._watched_nodes:
            self._watched_nodes[nid] = set([watch_handle])
            self._execute(self._logic.ensure_connected, nid, time.time())
        else:
            self._watched_nodes[nid].add(watch_handle)

    def unwatch_node(self, nid, watch_handle):
        try:
            self._watched_nodes[nid].discard(watch_handle)
        except KeyError:
            pass

    def stop(self):
        self.stop = lambda: None
        self.send_message = lambda nid, msg_h: None
        self.watch_node = lambda nid, watch_handle: None
        self.unwatch_node = lambda nid, watch_handle: None
        if hasattr(self, '_heartbeater'):
            self._heartbeater.kill()
            self._heartbeater = _DELETED
        if hasattr(self, '_listener_out'):
            self._listener_out.kill()
            self._listener_out = None
        if hasattr(self, '_listener_in'):
            self._listener_in.kill()
            self._listener_in = None
        if hasattr(self, '_initialized'):
            logic, self._logic = self._logic, None
            self._execute(logic.shutdown)
            sleep(.1)  # XXX: needed?
        if hasattr(self, '_ctx'):
            self._insock = self._outsock = None
            self._ctx.destroy(linger=0)
            self._ctx = None

    def __del__(self):
        self.stop()

    def __repr__(self):
        return "Hub(%s)" % (self.nid,)

    def _listen(self, sock, on_sock):
        recv, t, execute, message_received, ping_received, sig_disconnect_received = (
            sock.recv_multipart, time.time, self._execute, self._logic.message_received, self._logic.ping_received, self._logic.sig_disconnect_received)
        while True:
            data = recv()
            try:
                sender_nid, msg_bytes = data
            except ValueError:
                continue  # malformed input
            # dbg("recv", repr(msg_bytes), "from", sender_nid)
            msg_header, msg_bytes = msg_bytes[:4], msg_bytes[4:]
            if msg_header == SIG_DISCONNECT:
                assert not msg_bytes
                execute(sig_disconnect_received, sender_nid)
            elif msg_header == SIG_NEW_RELAY:
                assert not msg_bytes
                self._logic.new_relay_received(sender_nid)
            elif msg_header == SIG_RELAY_CONNECT:
                execute(self._logic.relay_connect_received, on_sock, relayer_nid=sender_nid, relayee_nid=msg_bytes)
            elif msg_header == SIG_RELAY_CONNECTED:
                execute(self._logic.relay_connected_received, relayee_nid=msg_bytes)
            elif msg_header == SIG_RELAY_NODEDOWN:
                execute(self._logic.relay_nodedown_received, relay_nid=sender_nid, relayee_nid=msg_bytes)
            elif msg_header == SIG_RELAY_SEND:
                relayee_nid, relayed_bytes = msg_bytes.split('\0', 1)
                execute(self._logic.relay_send_received, sender_nid, relayee_nid, relayed_bytes)
            elif msg_header == SIG_RELAY_FORWARDED:
                relayer_nid, relayed_bytes = msg_bytes.split('\0', 1)
                execute(self._logic.relay_forwarded_received, relayer_nid, relayed_bytes)
            elif msg_header == SIG_RELAY_NVM:
                execute(self._logic.relay_nvm_received, sender_nid, relayee_nid=msg_bytes)
            elif msg_header < MIN_VERSION_BITS:
                continue  # malformed input
            else:
                try:
                    unpacked = struct.unpack(MSG_HEADER_FORMAT, msg_header)
                except Exception:
                    continue  # malformed input
                version = unpacked[0] - MIN_VERSION_VALUE
                if msg_bytes:
                    execute(message_received, on_sock, sender_nid, version, msg_bytes, t())
                else:
                    execute(ping_received, on_sock, sender_nid, version, t())

    def _execute(self, fn, *args, **kwargs):
        g = fn(*args, **kwargs)
        if g is None:
            return
        insock_send, outsock_send, on_receive = self._insock.send_multipart, self._outsock.send_multipart, self._on_receive
        with self._lock:
            for action in flatten(g):
                cmd = action[0]
                # if cmd not in (NextBeat,):
                #     dbg("%s -> %s: %s" % (fn.__name__.ljust(25), cmd, ", ".join(repr(x) for x in action[1:])))
                if cmd is Send:
                    _, use_sock, nid, version, msg_h = action
                    (outsock_send if use_sock == OUT else insock_send)((nid, struct.pack(MSG_HEADER_FORMAT, MIN_VERSION_VALUE + version) + msg_h.serialize()))
                elif cmd is Receive:
                    _, sender_nid, msg_bytes = action
                    on_receive(sender_nid, msg_bytes)
                elif cmd is RelaySend:
                    _, use_sock, relay_nid, relayee_nid, msg_h = action
                    (outsock_send if use_sock == OUT else insock_send)((relay_nid, SIG_RELAY_SEND + relayee_nid + '\0' + msg_h.serialize()))
                elif cmd is RelayForward:
                    _, use_sock, recipient_nid, relayer_nid, relayed_bytes = action
                    (outsock_send if use_sock == OUT else insock_send)((recipient_nid, SIG_RELAY_FORWARDED + relayer_nid + '\0' + relayed_bytes))
                elif cmd is Ping:
                    _, use_sock, nid, version = action
                    (outsock_send if use_sock == OUT else insock_send)((nid, struct.pack(MSG_HEADER_FORMAT, MIN_VERSION_VALUE + version)))
                elif cmd is NextBeat:
                    _, time_to_next = action
                    if self._heartbeater is not _DELETED:
                        self._heartbeater = spawn_later(time_to_next, self._heartbeat)
                elif cmd is RelaySigNew:
                    _, use_sock, nid = action
                    (outsock_send if use_sock == OUT else insock_send)((nid, SIG_NEW_RELAY))
                elif cmd is RelayConnect:
                    _, use_sock, relay_nid, relayee_nid = action
                    (outsock_send if use_sock == OUT else insock_send)((relay_nid, SIG_RELAY_CONNECT + relayee_nid))
                elif cmd is RelaySigConnected:
                    _, use_sock, relayer_nid, relayee_nid = action
                    (outsock_send if use_sock == OUT else insock_send)((relayer_nid, SIG_RELAY_CONNECTED + relayee_nid))
                elif cmd is RelaySigNodeDown:
                    _, use_sock, relayer_nid, relayee_nid = action
                    (outsock_send if use_sock == OUT else insock_send)((relayer_nid, SIG_RELAY_NODEDOWN + relayee_nid))
                elif cmd is RelayNvm:
                    _, use_sock, relay_nid, relayee_nid = action
                    (outsock_send if use_sock == OUT else insock_send)((relay_nid, SIG_RELAY_NVM + relayee_nid))
                elif cmd is SendFailed:
                    _, msg_h = action
                    msg_h.send_failed()
                elif cmd is SigDisconnect:
                    _, use_sock, nid = action
                    (outsock_send if use_sock == OUT else insock_send)([nid, SIG_DISCONNECT])
                elif cmd is NodeDown:
                    _, nid = action
                    for watch_handle in self._watched_nodes.pop(nid, []):
                        self._on_node_down(watch_handle, nid)
                elif cmd is Connect:
                    _, naddr = action
                    if naddr not in self.FAKE_INACCESSIBLE_NADDRS:
                        zmqaddr = naddr_to_zmq_endpoint(naddr)
                        if zmqaddr:
                            self._outsock.connect(zmqaddr)
                        else:
                            pass  # TODO: would be nicer if we used this information and notified an immediate disconnect
                    sleep(0.001)
                elif cmd is Disconnect:
                    _, naddr = action
                    if naddr not in self.FAKE_INACCESSIBLE_NADDRS:
                        zmqaddr = naddr_to_zmq_endpoint(naddr)
                        if zmqaddr:
                            try:
                                self._outsock.disconnect(zmqaddr)
                            except zmq.ZMQError:
                                pass
                elif cmd is Bind:
                    _, naddr = action
                    zmqaddr = naddr_to_zmq_endpoint(naddr)
                    if not zmqaddr:
                        raise Exception("Failed to bind to %s" % (naddr,))
                    self._insock.bind(zmqaddr)
                else:
                    assert False, "unknown command: %r" % (cmd,)

    def _heartbeat(self):
        self._execute(self._logic.heartbeat, time.time())
verifyClass(IHub, Hub)


def naddr_to_zmq_endpoint(nid):
    if '\0' in nid:
        return None
    try:
        host, port = nid.split(':')
    except ValueError:
        return None
    try:
        return 'tcp://%s:%s' % (gethostbyname(host), port)
    except socket.gaierror as e:
        if e.errno != socket.EAI_NONAME:
            raise
        else:
            return None
