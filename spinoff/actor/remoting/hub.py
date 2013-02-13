from __future__ import print_function

import time
import struct
import cPickle as pickle
from types import GeneratorType

import zmq.green as zmq
from gevent import sleep, spawn, spawn_later
from gevent.socket import gethostbyname

from spinoff.actor.events import Events, DeadLetter
from spinoff.actor.remoting.hublogic import (
    HubLogic, Connect, Disconnect, SigDisconnect, Send, Ping,
    RelaySigNew, RelayConnect, RelaySigConnected, RelaySend, RelayForward, RelaySigNodeDown, RelayNvm,
    Receive, SendFailed, NodeDown, IN, OUT)
from spinoff.util.logging import dbg


__all__ = ['Hub']


MSG_HEADER_FORMAT = '!I'
_signals = [struct.pack(MSG_HEADER_FORMAT, x) for x in range(8)]
SIG_DISCONNECT, SIG_NEW_RELAY, SIG_RELAY_CONNECT, SIG_RELAY_CONNECTED, SIG_RELAY_SEND, SIG_RELAY_FORWARDED, SIG_RELAY_NODEDOWN, SIG_RELAY_NVM = _signals
min_version_bits = len(_signals)
MIN_VERSION_BITS = struct.pack(MSG_HEADER_FORMAT, min_version_bits)


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """
    __doc_HEARTBEAT_INTERVAL__ = (
        "Time on seconds after which to send out a heartbeat signal to all known nodes. Regular messages can be "
        "subsituted by the framework for heartbeats to save network bandwidth.")
    HEARTBEAT_INTERVAL = 1.0
    ALLOWED_HEARTBEAT_DELAY = HEARTBEAT_INTERVAL * 0.2

    HEARTBEAT_MAX_SILENCE = 3.0
    RELAY_NOTIFY_INTERVAL = 30.0

    FAKE_INACCESSIBLE_NODES = set()

    def __init__(self, nid, is_relay=False, on_node_down=lambda ref, nid: ref << ('_node_down', nid),
                 on_receive=lambda sender_nid, msg_h: print("deliver", msg_h, "from", sender_nid)):
        self.is_relay = is_relay
        self._on_node_down = on_node_down
        self._on_receive = on_receive
        self._logic = HubLogic(is_relay=is_relay,
                               heartbeat_interval=self.HEARTBEAT_INTERVAL,
                               heartbeat_max_silence=self.HEARTBEAT_MAX_SILENCE)
        self._ctx = zmq.Context()
        self._ctx.linger = 0
        self._insock = self._ctx.socket(zmq.ROUTER)
        self._outsock = self._ctx.socket(zmq.ROUTER)
        self._insock.identity = self._outsock.identity = nid
        self._insock.bind(nid_to_zmq_endpoint(nid))
        self._listener_in = spawn(self._listen, self._insock, IN)
        self._listener_in.link(lambda _: self.stop())
        self._listener_out = spawn(self._listen, self._outsock, OUT)
        self._listener_out.link(lambda _: self.stop())
        self._heartbeater = spawn(self._send_recv_heartbeat)
        self._heartbeater.link(lambda _: self.stop())
        self._watched_nodes = {}
        self._initialized = True

    def _listen(self, sock, on_sock):
        recv, t, execute, message_received, ping_received, sig_disconnect_received = (
            sock.recv_multipart, time.time, self._execute, self._logic.message_received, self._logic.ping_received, self._logic.sig_disconnect_received)
        while True:
            sender_nid, msg_bytes = recv()
            # dbg("recv", repr(msg_bytes), "from", sender_nid)
            msg_header, msg_bytes = msg_bytes[:4], msg_bytes[4:]
            if msg_header == SIG_DISCONNECT:
                assert not msg_bytes
                execute(sig_disconnect_received, sender_nid)
            elif msg_header == SIG_NEW_RELAY:
                assert not msg_bytes
                self._logic.new_relay_received(sender_nid)
            elif msg_header == SIG_RELAY_CONNECT:
                execute(self._logic.relay_connect_received, requestor_nid=sender_nid, proxied_nid=msg_bytes)
            elif msg_header == SIG_RELAY_CONNECTED:
                execute(self._logic.relay_connected_received, proxied_nid=msg_bytes)
            elif msg_header == SIG_RELAY_NODEDOWN:
                execute(self._logic.relay_nodedown_received, relay_nid=sender_nid, proxied_nid=msg_bytes)
            elif msg_header == SIG_RELAY_SEND:
                actual_recipient_nid, actual_msg_bytes = msg_bytes.split('\0', 1)
                execute(self._logic.relay_send_received, sender_nid, actual_recipient_nid, actual_msg_bytes)
            elif msg_header == SIG_RELAY_FORWARDED:
                actual_sender_nid, actual_msg_bytes = msg_bytes.split('\0', 1)
                execute(self._logic.relay_forwarded_received, actual_sender_nid, actual_msg_bytes)
            elif msg_header == SIG_RELAY_NVM:
                execute(self._logic.relay_nvm_received, sender_nid, relayed_nid=msg_bytes)
            elif msg_header < MIN_VERSION_BITS:
                raise NotImplementedError("don't know how to handle signal: %r" % (msg_header,))
            else:
                version = struct.unpack(MSG_HEADER_FORMAT, msg_header)
                if msg_bytes:
                    execute(message_received, on_sock, sender_nid, version, msg_bytes, t())
                else:
                    execute(ping_received, on_sock, sender_nid, version, t())

    def _send_recv_heartbeat(self):
        interval, heartbeat, execute, t = self.HEARTBEAT_INTERVAL, self._logic.heartbeat, self._execute, time.time
        while True:
            execute(heartbeat, t())
            sleep(interval)

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
        except IndexError:
            pass

    def _execute(self, fn, *args, **kwargs):
        for action in flatten(fn(*args, **kwargs)):
            cmd = action[0]
            if cmd is not Ping:
                dbg("%s -> %s: %s" % (fn.__name__.ljust(25), cmd, ", ".join(repr(x) for x in action[1:])))
            if cmd is Send:
                _, use_sock, nid, version, msg_h = action
                if use_sock == IN or nid not in self.FAKE_INACCESSIBLE_NODES:
                    (self._outsock if use_sock == OUT else self._insock).send_multipart([nid, struct.pack(MSG_HEADER_FORMAT, min_version_bits + version) + msg_h.serialize()])
            elif cmd is Ping:
                _, use_sock, nid, version = action
                if use_sock == IN or nid not in self.FAKE_INACCESSIBLE_NODES:
                    (self._outsock if use_sock == OUT else self._insock).send_multipart([nid, struct.pack(MSG_HEADER_FORMAT, min_version_bits + version)])
            elif cmd is Connect:
                _, nid = action
                if nid not in self.FAKE_INACCESSIBLE_NODES:
                    self._outsock.connect(nid_to_zmq_endpoint(nid))
                    sleep(.01)
            elif cmd is Disconnect:
                _, nid = action
                if nid not in self.FAKE_INACCESSIBLE_NODES:
                    self._outsock.disconnect(nid_to_zmq_endpoint(nid))
            elif cmd is SigDisconnect:
                _, use_sock, nid = action
                if use_sock == IN or nid not in self.FAKE_INACCESSIBLE_NODES:
                    (self._outsock if use_sock == OUT else self._insock).send_multipart([nid, SIG_DISCONNECT])
            elif cmd is RelaySigNew:
                _, use_sock, nid = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([nid, SIG_NEW_RELAY])
            elif cmd is RelayConnect:
                _, use_sock, relay_nid, proxied_nid = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([relay_nid, SIG_RELAY_CONNECT + proxied_nid])
            elif cmd is RelaySigConnected:
                _, use_sock, requestor_nid, proxied_nid = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([requestor_nid, SIG_RELAY_CONNECTED + proxied_nid])
            elif cmd is RelaySigNodeDown:
                _, use_sock, requestor_nid, proxied_nid = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([requestor_nid, SIG_RELAY_NODEDOWN + proxied_nid])
            elif cmd is RelaySend:
                _, use_sock, relay_nid, proxied_nid, msg_h = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([relay_nid, SIG_RELAY_SEND + proxied_nid + '\0' + msg_h.serialize()])
            elif cmd is RelayForward:
                _, use_sock, recipient_nid, actual_sender_nid, actual_msg_bytes = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([recipient_nid, SIG_RELAY_FORWARDED + actual_sender_nid + '\0' + actual_msg_bytes])
            elif cmd is RelayNvm:
                _, use_sock, relay_nid, proxied_nid = action
                (self._outsock if use_sock == OUT else self._insock).send_multipart([relay_nid, SIG_RELAY_NVM + proxied_nid])
            elif cmd is Receive:
                _, sender_nid, msg_h = action
                self._on_receive(sender_nid, msg_h)
            elif cmd is SendFailed:
                _, msg_h = action
                msg_h.send_failed()
            elif cmd is NodeDown:
                _, nid = action
                for watch_handle in self._watched_nodes.pop(nid, []):
                    self._on_node_down(watch_handle, nid)
            else:
                assert False, "unknown command: %r" % (cmd,)

    def stop(self):
        if getattr(self, '_heartbeater', None):
            self._heartbeater.kill()
            self._heartbeater = None
        if getattr(self, '_listener_out', None):
            self._listener_out.kill()
            self._listener_out = None
        if getattr(self, '_listener_in', None):
            self._listener_in.kill()
            self._listener_in = None
        if getattr(self, '_initialized', None):
            self._execute(self._logic.shutdown)
            self._logic = self._initialized = None
            sleep(.01)  # XXX: needed?
        if getattr(self, '_ctx', None):
            self._insock = self._outsock = None
            self._ctx.destroy(linger=0)
            self._ctx = None

    def __del__(self):
        self.stop()

    def __repr__(self):
        return "Hub(%s)" % (self._insock.identity,)


def nid_to_zmq_endpoint(nid):
    host, port = nid.split(':')
    return 'tcp://%s:%s' % (gethostbyname(host), port)


def flatten(gen):
    for x in gen:
        if isinstance(x, GeneratorType):
            for y in flatten(x):
                yield y
        else:
            yield x


class Msg(object):
    def __init__(self, ref, msg):
        self.ref, self.msg = ref, msg

    def serialize(self):
        return pickle.dumps((self.ref.uri.path, self.msg), protocol=2)

    def send_failed(self):
        Events.log(DeadLetter(self.ref, self.msg))
