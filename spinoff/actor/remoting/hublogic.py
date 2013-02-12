from __future__ import print_function

import struct

from spinoff.util.python import enumrange
from spinoff.util.logging import dbg


DISCONNECT, MESSAGE = 1, 2
MSG_HEADER_FORMAT = '!I'
NO_CHANGE = object()

DISCONNECT_SIGNAL = '\0\0\0\0'


# signals from HubLogic to Hub or test code
Connect, Disconnect, SendPacket, Deliver, NotifySendFailed, NotifyNodeDown = enumrange('Connect', 'Disconnect', 'SendPacket', 'Deliver', 'NotifySendFailed', 'NotifyNodeDown')


class HubLogic(object):
    def __init__(self, heartbeat_interval, heartbeat_max_silence):
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_max_silence = heartbeat_max_silence
        self.channels = set()
        self.last_seen = {}
        self.last_sent = {}
        self.versions = {}
        self.queues = {}
        # this gets incremented before being used, so we actually start from 1 not 0; this is so that a 0 byte at the
        # beginning of a pack-*et could be identified as a DISCONNECT
        self.version = 0

    def _needs_ping(self, node_id, t):
        ret = self.last_sent.get(node_id, 0) <= t - self.heartbeat_interval
        if ret:
            self.last_sent[node_id] = t
        return ret

    def send_message(self, recipient_node_id, msg_handle, current_time):
        # is it a new connection, or an existing but not yet active connection?
        if recipient_node_id in self.channels:
            if self._is_connection_active(recipient_node_id):
                yield SendPacket, recipient_node_id, self._make_ping_packet() + msg_handle.serialise()
            else:
                self.queues[recipient_node_id].append(msg_handle)
        else:
            yield Connect, recipient_node_id
            if self._needs_ping(recipient_node_id, current_time):
                yield SendPacket, recipient_node_id, self._make_ping_packet()
            self.channels.add(recipient_node_id)
            self.last_seen[recipient_node_id] = current_time
            self.queues.setdefault(recipient_node_id, []).append(msg_handle)

    def message_received(self, sender_node_id, msg_bytes, current_time):
        if msg_bytes == DISCONNECT_SIGNAL:
            dbg("DISCONNECT RECEIVED")
            if sender_node_id in self.channels:
                yield Disconnect, sender_node_id
                self.channels.remove(sender_node_id)
                yield NotifyNodeDown, sender_node_id
                for msg_handle in self.queues.pop(sender_node_id, []):
                    yield NotifySendFailed, msg_handle
        else:
            version, msg_body_bytes = struct.unpack(MSG_HEADER_FORMAT, msg_bytes[:4]), msg_bytes[4:]
            if sender_node_id not in self.channels:
                self.channels.add(sender_node_id)
                self.last_seen[sender_node_id] = current_time
                if self._needs_ping(sender_node_id, current_time):
                    yield SendPacket, sender_node_id, self._make_ping_packet()
            else:
                if sender_node_id in self.queues:
                    for msg_handle in self.queues.pop(sender_node_id, []):
                        yield SendPacket, sender_node_id, self._make_ping_packet() + msg_handle.serialise()
                else:
                    if not (version > self.versions.get(sender_node_id, 0)):
                        # version has been reset--he has restarted, so emulate a node-down-node-back-up event pair:
                        yield NotifyNodeDown, sender_node_id
                        self.versions[sender_node_id] = version
                        assert sender_node_id not in self.queues
            self.versions[sender_node_id] = version
            self.last_seen[sender_node_id] = current_time
            if msg_body_bytes:
                yield Deliver, msg_body_bytes, sender_node_id

    def heartbeat(self, current_time):
        t_gone = current_time - self.heartbeat_max_silence
        removed = set()
        for node_id in self.channels:
            if self.last_seen[node_id] < t_gone:
                yield Disconnect, node_id
                removed.add(node_id)
                #
                for msg_handle in self.queues.pop(node_id, []):
                    yield NotifySendFailed, msg_handle
                yield NotifyNodeDown, node_id
            else:
                if self._needs_ping(node_id, current_time):
                    yield SendPacket, node_id, self._make_ping_packet()
        self.channels -= removed

    def watch_new_node(self, node_id, current_time):
        # open fresh channel
        yield Connect, node_id
        yield SendPacket, node_id, self._make_ping_packet()
        self.last_seen[node_id] = current_time
        self.channels.add(node_id)
        self.queues[node_id] = []

    def shutdown(self):
        for node_id in self.channels:
            yield SendPacket, node_id, DISCONNECT_SIGNAL
            for msg_handle in self.queues.pop(node_id, []):
                yield NotifySendFailed, msg_handle
            yield NotifyNodeDown, node_id
        self.channels.clear()

    # private:

    def _make_ping_packet(self):
        self.version += 1
        return struct.pack(MSG_HEADER_FORMAT, self.version)

    def _is_connection_active(self, node_id):
        return node_id in self.versions
