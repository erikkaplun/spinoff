from __future__ import print_function

from itertools import chain

from spinoff.util.python import enumrange
from spinoff.util.logging import dbg


# signals from HubLogic to Hub or test code
Connect, Disconnect, SigDisconnect, Send, Ping, Receive, SendFailed, NodeDown = enumrange('Connect', 'Disconnect', 'SigDisconnect', 'Send', 'Ping', 'Receive', 'SendFailed', 'NodeDown')
IN, OUT = enumrange('IN', 'OUT')


class HubLogic(object):
    def __init__(self, heartbeat_interval, heartbeat_max_silence):
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_max_silence = heartbeat_max_silence
        self.channels_in = set()
        self.channels_out = set()
        self.last_seen = {}
        self.last_sent = {}
        self.versions = {}
        self.queues = {}
        # this gets incremented before being used, so we actually start from 1 not 0; this is so that a 0 byte at the
        # beginning of a pack-*et could be identified as a DISCONNECT
        self.version = 0

    def send_message(self, recipient_node_id, msg_handle, current_time):
        # is it a new connection, or an existing but not yet active connection?
        if recipient_node_id in self.channels_in:
            yield Send, IN, recipient_node_id, self._next_version(), msg_handle
        elif recipient_node_id in self.channels_out:
            if recipient_node_id not in self.queues:
                yield Send, OUT, recipient_node_id, self._next_version(), msg_handle
            else:
                self.queues[recipient_node_id].append(msg_handle)
        else:
            yield Connect, recipient_node_id
            if self._needs_ping(recipient_node_id, current_time):
                yield Ping, OUT, recipient_node_id, self._next_version()
            self.channels_out.add(recipient_node_id)
            self.last_seen[recipient_node_id] = current_time
            self.queues.setdefault(recipient_node_id, []).append(msg_handle)

    def sig_disconnect_received(self, sender_node_id):
        if sender_node_id in self.channels_in:
            self.channels_in.remove(sender_node_id)
        elif sender_node_id in self.channels_out:
            self.channels_out.remove(sender_node_id)
        else:
            return
        yield NodeDown, sender_node_id
        for msg_handle in self.queues.pop(sender_node_id, []):
            yield SendFailed, msg_handle

    def ping_received(self, dir, sender_node_id, version, current_time):
        self.last_seen[sender_node_id] = current_time
        if dir == IN:
            self.channels_in.add(sender_node_id)
        if dir == OUT:
            self.channels_out.add(sender_node_id)
        inout = (IN if sender_node_id in self.channels_in else OUT)
        if self._needs_ping(sender_node_id, current_time):
            yield Ping, inout, sender_node_id, self._next_version()
        if sender_node_id in self.queues:
            for msg_handle in self.queues.pop(sender_node_id):
                yield Send, inout, sender_node_id, self._next_version(), msg_handle
        else:
            if not (version > self.versions.get(sender_node_id, 0)):
                # version has been reset--he has restarted, so emulate a node-down-node-back-up event pair:
                yield NodeDown, sender_node_id
                self.versions[sender_node_id] = version
                assert sender_node_id not in self.queues
        self.versions[sender_node_id] = version

    def message_received(self, dir, sender_node_id, version, msg_body_bytes, current_time):
        yield self.ping_received(dir, sender_node_id, version, current_time)
        if msg_body_bytes:
            yield Receive, msg_body_bytes, sender_node_id

    def heartbeat(self, current_time):
        t_gone = current_time - self.heartbeat_max_silence
        removed = set()
        for node_id in chain(self.channels_in, self.channels_out):
            if self.last_seen[node_id] < t_gone:
                yield Disconnect, node_id
                removed.add(node_id)
                for msg_handle in self.queues.pop(node_id, []):
                    yield SendFailed, msg_handle
                yield NodeDown, node_id
            else:
                if self._needs_ping(node_id, current_time):
                    yield Ping, (IN if node_id in self.channels_in else OUT), node_id, self._next_version()
        self.channels_in -= removed
        self.channels_out -= removed

    def ensure_connected(self, node_id, current_time):
        # open fresh channel
        if node_id not in self.channels_in and node_id not in self.channels_out:
            yield Connect, node_id
            yield Ping, OUT, node_id, self._next_version()
            self.last_seen[node_id] = current_time
            self.channels_out.add(node_id)
            self.queues[node_id] = []

    def shutdown(self):
        for node_id in chain(self.channels_in, self.channels_out):
            yield SigDisconnect, (IN if node_id in self.channels_in else OUT), node_id
            for msg_handle in self.queues.pop(node_id, []):
                yield SendFailed, msg_handle
        self.channels_in = self.channels_out = None

    # private:

    def _needs_ping(self, node_id, t):
        ret = self.last_sent.get(node_id, 0) <= t - self.heartbeat_interval
        if ret:
            self.last_sent[node_id] = t
        return ret

    def _next_version(self):
        self.version += 1
        return self.version

    def __repr__(self):
        return "HubLogic()"
