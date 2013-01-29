from __future__ import print_function

import struct
import cPickle as pickle
from cStringIO import StringIO

from .pickler import IncomingMessageUnpickler


DISCONNECT, MESSAGE = 1, 2
MSG_HEADER_FORMAT = '!I'
NO_CHANGE = object()

DISCONNECT_SIGNAL = '\0\0\0\0'


class HubLogic(object):
    def __init__(self, node_id, hub):
        self.channels = set()
        self.last_seen = {}
        self.versions = {}
        self.queues = {}
        self.node_id = node_id
        self.hub = hub
        # we start from 1 not 0 so that a 0 byte at the beginning of a pack-*et could be identified as a DISCONNECT
        self.version = 1

    def message_received(self, sender_node_id, msg_bytes, current_time):
        if msg_bytes == DISCONNECT_SIGNAL:
            if sender_node_id in self.channels:
                yield Actions.DestroyChannel(sender_node_id)
                self._channel_destroyed(sender_node_id)
        else:
            version, msg_body_bytes = struct.unpack(MSG_HEADER_FORMAT, msg_bytes[:4]), msg_bytes[4:]
            if sender_node_id not in self.channels:
                yield Actions.MakeChannel(sender_node_id)
                yield Actions.SendPacket(sender_node_id, self._make_ping_packet())
                self._channel_created(sender_node_id)
            else:
                if not self._is_connection_active(sender_node_id):
                    for x in self.queues.pop(sender_node_id):
                        yield Actions.SendPacket(sender_node_id, self._serialize(x))
                else:
                    if not (version > self.versions.get(sender_node_id, 0)):
                        # version has been reset--he has restarted, so emulate a node-down-node-back-up event pair:
                        self._channel_destroyed(sender_node_id)
                        self._channel_created(sender_node_id)

            self.versions[sender_node_id] = version
            self.last_seen[sender_node_id] = current_time

            if msg_body_bytes:
                local_path, message = self._deserialize(msg_bytes)
                yield Actions.DeliverLocal(local_path, message, sender_node_id)

    def heartbeat(self, current_time):
        t_gone = current_time - self.heartbeat_max_silence
        for node_id in self.channels:
            if self.last_seen[node_id] < t_gone:
                yield Actions.SendPacket(node_id, DISCONNECT_SIGNAL)  # XXX: is it a good idea to DISCONNECT here?
                yield Actions.DestroyChannel(node_id)
                self._channel_destroyed(node_id)
            else:
                yield Actions.SendPacket(node_id, self._make_ping_packet())
        self.version += 1

    def _make_ping_packet(self):
        return struct.pack(MSG_HEADER_FORMAT, self.version)

    def send_message(self, recipient_node_id, recipient_ref, message):
        # is it a new connection, or an existing but not yet active connection?
        if recipient_node_id in self.channels and self._is_connection_active(recipient_node_id):
            yield Actions.SendPacket(recipient_node_id, self._serialize((recipient_ref, message)))
        else:
            if recipient_node_id not in self.channels:
                yield Actions.MakeChannel(recipient_node_id)
                yield Actions.SendPacket(recipient_node_id, self._make_ping_packet())
                self.queues[recipient_node_id] = []
            self.queues[recipient_node_id].append((recipient_node_id, recipient_ref, message))

    def watch_node(self, node_id, watcher):
        if node_id not in self.channels:
            yield Actions.MakeChannel(node_id)
            yield Actions.SendPacket(node_id, self._make_ping_packet())
            self._channel_created(node_id)
        self.death_watch.setdefault(node_id, set()).add(watcher)

    def unwatch_node(self, node_id, watcher):
        self.death_watch.pop(node_id, set()).discard(watcher)

    def shutdown(self):
        for node_id in self.channels:
            self._channel_destroyed(node_id)
            yield Actions.SendPacket(node_id, DISCONNECT_SIGNAL)

    # private:

    def _channel_created(self, node_id):
        self.queues[node_id] = []
        self.channels.add(node_id)

    def _channel_destroyed(self, node_id):
        for ref, msg in self.queues.pop(node_id, []):
            yield Actions.NotifyDeadLetter(ref, msg)
        node_id = self.node_id_to_nodeid[node_id]
        for x in self.death_watch.pop(node_id, []):
            x << ('_node_down', node_id)
        self.channels.remove(node_id)

    def _is_connection_active(self, node_id):
        return node_id in self.versions

    def _deserialize(self, data):
        return IncomingMessageUnpickler(self.hub, StringIO(data)).load()

    def _serialize(self, (ref, msg)):
        return pickle.dumps((ref.uri.path, msg), protocol=2)


class Actions:
    MakeChannel = classmethod(lambda node_id: ('make-channel', node_id))
    DestroyChannel = classmethod(lambda node_id: ('destroy-channel', node_id))
    SendPacket = classmethod(lambda node_id, recipient_ref, message: ('send', node_id, recipient_ref, message))

    DeliverLocal = classmethod(lambda path, message, sender_node_id: ('deliver', path, message, sender_node_id))

    NotifyDeadLetter = classmethod(lambda orig_recipient, orig_message: ('notify-dead-letter', orig_recipient, orig_message))
    NotifyRemoteDeadLetter = classmethod(lambda local_path, message, sender_node_id: ('notify-remote-dead-letter', local_path, message, sender_node_id))
    NotifyNodeDown = classmethod(lambda local_ref, node_id: ('notify-node-down', local_ref, node_id))
