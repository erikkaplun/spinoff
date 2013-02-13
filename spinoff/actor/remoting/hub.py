from __future__ import print_function

import time
import struct
import cPickle as pickle
from types import GeneratorType

import zmq.green as zmq
from gevent import sleep, spawn
from gevent.socket import gethostbyname

from spinoff.actor.events import Events, DeadLetter
from spinoff.actor.remoting.hublogic import HubLogic, Connect, Disconnect, SigDisconnect, Send, Ping, Receive, SendFailed, NodeDown, IN, OUT
from spinoff.util.logging import dbg


__all__ = ['Hub']


MSG_HEADER_FORMAT = '!I'
DISCONNECT_SIGNAL = '\0\0\0\0'


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

    def __init__(self, node_id, on_node_down=lambda ref, node_id: ref << ('_node_down', node_id),
                 on_receive=lambda msg_bytes, sender_node_id: print("deliver", msg_bytes, "from", sender_node_id)):
        self._on_node_down = on_node_down
        self._on_receive = on_receive
        self._logic = HubLogic(heartbeat_interval=self.HEARTBEAT_INTERVAL,
                               heartbeat_max_silence=self.HEARTBEAT_MAX_SILENCE)
        self._ctx = zmq.Context()
        self._ctx.linger = 0
        self._insock = self._ctx.socket(zmq.ROUTER)
        self._outsock = self._ctx.socket(zmq.ROUTER)
        self._insock.identity = self._outsock.identity = node_id
        self._insock.bind(node_id_to_zmq_endpoint(node_id))
        self._listener_in = spawn(self._listen, self._insock, IN)
        self._listener_out = spawn(self._listen, self._outsock, OUT)
        self._heartbeater = spawn(self._send_recv_heartbeat)
        self._watched_nodes = {}
        self._initialized = True

    def _listen(self, sock, dir):
        recv, t, execute, message_received, ping_received, sig_disconnect_received = (
            sock.recv_multipart, time.time, self._execute, self._logic.message_received, self._logic.ping_received, self._logic.sig_disconnect_received)
        while True:
            sender_node_id, msg_bytes = recv()
            if msg_bytes == DISCONNECT_SIGNAL:
                execute(sig_disconnect_received, sender_node_id)
            else:
                version, msg_body_bytes = struct.unpack(MSG_HEADER_FORMAT, msg_bytes[:4]), msg_bytes[4:]
                if msg_body_bytes:
                    execute(message_received, dir, sender_node_id, version, msg_body_bytes, t())
                else:
                    execute(ping_received, dir, sender_node_id, version, t())

    def _send_recv_heartbeat(self):
        interval, heartbeat, execute, t = self.HEARTBEAT_INTERVAL, self._logic.heartbeat, self._execute, time.time
        while True:
            execute(heartbeat, t())
            sleep(interval)

    def send_message(self, node_id, msg_handle):
        self._execute(self._logic.send_message, node_id, msg_handle, time.time())

    def watch_node(self, node_id, watch_handle):
        if node_id not in self._watched_nodes:
            self._watched_nodes[node_id] = set([watch_handle])
            self._execute(self._logic.ensure_connected, node_id, time.time())
        else:
            self._watched_nodes[node_id].add(watch_handle)

    def unwatch_node(self, node_id, watch_handle):
        try:
            self._watched_nodes[node_id].discard(watch_handle)
        except IndexError:
            pass

    def stop(self):
        if hasattr(self, '_heartbeater'):
            self._heartbeater.kill()
            del self._heartbeater
        if hasattr(self, '_listener_out'):
            self._listener_out.kill()
            del self._listener_out
        if hasattr(self, '_listener_in'):
            self._listener_in.kill()
            del self._listener_in
        if hasattr(self, '_initialized'):
            self._execute(self._logic.shutdown)
            del self._logic, self._initialized
            sleep(.01)  # XXX: needed?
        if hasattr(self, '_ctx'):
            self._ctx.destroy(linger=0)
            del self._ctx, self._insock

    def _execute(self, fn, *args, **kwargs):
        for action in flatten(fn(*args, **kwargs)):
            cmd = action[0]
            dbg("%s -> %s: %s" % (fn.__name__, cmd, ", ".join(repr(x) for x in action[1:])))
            if cmd is Send:
                _, dir, node_id, version, msg_handle = action
                (self._outsock if dir == OUT else self._insock).send_multipart([node_id, struct.pack(MSG_HEADER_FORMAT, version) + msg_handle.serialise()])
            elif cmd is Ping:
                _, dir, node_id, version = action
                (self._outsock if dir == OUT else self._insock).send_multipart([node_id, struct.pack(MSG_HEADER_FORMAT, version)])
            elif cmd is SigDisconnect:
                _, dir, node_id = action
                (self._outsock if dir == OUT else self._insock).send_multipart([node_id, DISCONNECT_SIGNAL])
            elif cmd is Connect:
                _, node_id = action
                self._outsock.connect(node_id_to_zmq_endpoint(node_id))
                sleep(.01)
            elif cmd is Disconnect:
                _, node_id = action
                self._outsock.disconnect(node_id_to_zmq_endpoint(node_id))
            elif cmd is SigDisconnect:
                _, dir, node_id = action
                (self._outsock if dir == OUT else self._insock).send_multipart([node_id, DISCONNECT_SIGNAL])
            elif cmd is Receive:
                _, msg_bytes, sender_node_id = action
                self._on_receive(msg_bytes, sender_node_id)
            elif cmd is SendFailed:
                _, msg_handle = action
                msg_handle.send_failed()
            elif cmd is NodeDown:
                _, node_id = action
                for watch_handle in self._watched_nodes.pop(node_id, []):
                    self._on_node_down(watch_handle, node_id)
            else:
                assert False, "unknown command: %r" % (cmd,)

    def __del__(self):
        self.stop()

    def __repr__(self):
        return "Hub(%s)" % (self._insock.identity,)


def node_id_to_zmq_endpoint(node_id):
    host, port = node_id.split(':')
    return 'tcp://%s:%s' % (gethostbyname(host), port)


def flatten(gen):
    for x in gen:
        if isinstance(x, GeneratorType):
            for y in x:
                yield y
        else:
            yield x


class Msg(object):
    def __init__(self, ref, msg):
        self.ref, self.msg = ref, msg

    def serialise(self):
        return pickle.dumps((self.ref.uri.path, self.msg), protocol=2)

    def send_failed(self):
        Events.log(DeadLetter(self.ref, self.msg))
