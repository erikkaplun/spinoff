from __future__ import print_function

import time
import cPickle as pickle
from types import GeneratorType

import zmq.green as zmq
from gevent import sleep, spawn
from gevent.socket import gethostbyname

from spinoff.actor.events import Events, DeadLetter
from spinoff.actor.remoting.hublogic import HubLogic, Connect, Disconnect, SendPacket, Deliver, NotifySendFailed, NotifyNodeDown
from spinoff.util.logging import dbg


__all__ = ['Hub']


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

    def __init__(self, node_id, node_down_fn=lambda ref, node_id: ref << ('_node_down', node_id),
                 deliver_fn=lambda msg_bytes, sender_node_id: print("deliver", msg_bytes, "from", sender_node_id)):
        self._node_down_fn = node_down_fn
        self._deliver_fn = deliver_fn
        self._logic = HubLogic(heartbeat_interval=self.HEARTBEAT_INTERVAL,
                               heartbeat_max_silence=self.HEARTBEAT_MAX_SILENCE)
        self._ctx = zmq.Context()
        self._ctx.linger = 0
        self._sock = self._ctx.socket(zmq.ROUTER)
        self._sock.identity = node_id
        self._sock.bind(node_id_to_zmq_endpoint(node_id))
        self._listener = spawn(self._listen)
        self._hearbeater = spawn(self._send_recv_heartbeat)
        self._watched_nodes = {}

    def _listen(self):
        recv, message_received, execute, t = self._sock.recv_multipart, self._logic.message_received, self._execute, time.time
        while True:
            sender_node_id, msg_bytes = recv()
            execute(message_received(sender_node_id, msg_bytes, t()))

    def _send_recv_heartbeat(self):
        interval, heartbeat, execute, t = self.HEARTBEAT_INTERVAL, self._logic.heartbeat, self._execute, time.time
        while True:
            execute(heartbeat(t()))
            sleep(interval)

    def send_message(self, node_id, msg_handle):
        self._execute(self._logic.send_message(node_id, msg_handle, time.time()))

    def watch_node(self, node_id, watcher):
        if node_id not in self._watched_nodes:
            self._watched_nodes[node_id] = set([watcher])
            self._execute(self._logic.watch_new_node(node_id, time.time()))
        else:
            self._watched_nodes[node_id].add(watcher)

    def unwatch_node(self, node_id, watcher):
        try:
            self._watched_nodes[node_id].discard(watcher)
        except IndexError:
            pass

    def stop(self):
        self._listener.kill()
        self._execute(self._logic.shutdown())
        sleep(.01)  # XXX: needed?
        self._sock.shutdown()
        self._ctx.shutdown()

    def _execute(self, actions):
        for action in flatten(actions):
            cmd = action[0]
            dbg("%s: %s" % (cmd, ", ".join(repr(x) for x in action[1:])))
            if cmd is SendPacket:
                _, node_id, data = action
                self._sock.send_multipart([node_id, data])
            elif cmd is Connect:
                _, node_id = action
                self._sock.connect(node_id_to_zmq_endpoint(node_id))
            elif cmd is Disconnect:
                _, node_id = action
                try:
                    self._sock.disconnect(node_id_to_zmq_endpoint(node_id))
                except zmq.ZMQError:
                    pass
            elif cmd is Deliver:
                _, msg_bytes, sender_node_id = action
                self._deliver_fn(msg_bytes, sender_node_id)
            elif cmd is NotifySendFailed:
                _, msg_handle = action
                msg_handle.send_failed()
            elif cmd is NotifyNodeDown:
                _, node_id = action
                for watcher in self._watched_nodes.pop(node_id, []):
                    self._node_down_fn(watcher, node_id)
            else:
                assert False, "unknown command: %r" % (cmd,)

    def __repr__(self):
        return "Hub(%s)" % (self._sock.identity,)


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
