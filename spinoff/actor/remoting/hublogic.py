from __future__ import print_function

import random

from spinoff.util.python import enumrange
from spinoff.util.logging import dbg, err


# signals from HubLogic to Hub or test code
(
    Connect, Disconnect, SigDisconnect, Send, Ping,
    RelaySigNew, RelayConnect, RelaySigConnected, RelaySend, RelayForward, RelaySigNodeDown, RelayNvm,
    Receive, SendFailed, NodeDown
) = enumrange(
    'Connect', 'Disconnect', 'SigDisconnect', 'Send', 'Ping',
    'RelaySigNew', 'RelayConnect', 'RelaySigConnected', 'RelaySend', 'RelayForward', 'RelaySigNodeDown', 'RelayNvm',
    'Receive', 'SendFailed', 'NodeDown'
)
IN, OUT = enumrange('IN', 'OUT')


def FLUSH(self, nid):
    for msg_h in self.queues.pop(nid, []):
        yield SendFailed, msg_h


def NODEDOWN(self, nid):
    for x in FLUSH(self, nid):
        yield x
    yield NodeDown, nid


def RELAY_CHECKS(self, nid):
    if nid in self.rl_relayed:
        for requestor_nid in self.rl_relayed.pop(nid):
            requestors_proxied_nids = self.rl_requestors[requestor_nid]
            requestors_proxied_nids.remove(nid)
            if not requestors_proxied_nids:
                del self.rl_requestors[requestor_nid]
            yield RelaySigNodeDown, IN, requestor_nid, nid
    elif nid in self.rl_requestors:
        requestors_proxied_nids = self.rl_requestors.pop(nid)
        for x in requestors_proxied_nids:
            del self.rl_relayed[x]


class HubLogic(object):
    """The relaying logic assumes the correctness of propagated relay information and thus also sends RelaySigNew signals
    on OUT sockets, assuming IN is also possible on demand. Furthermore, all nodes are ready to accept relay requests,
    without checking if they have been marked to be able to do so. Therefore, marking a node as a relay only serves the
    purpose of ensuring correct relay information is propagated and does not guard against invalid relay requests coming
    from nodes whos relay flag has been set erroneously. This shold however not compromise the stability or security of
    the system, but a constant stream of failed relay packets probably will have serious performance implications. This
    is however easy to work around by having each node do a sanity check of its declared relaying capability at
    initialization.

    """
    def __init__(self, is_relay, heartbeat_interval, heartbeat_max_silence):
        self.is_relay = is_relay
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_max_silence = heartbeat_max_silence
        self.channels_in = set()
        self.channels_out = set()
        self.last_seen = {}
        self.last_sent = {}
        self.versions = {}
        self.queues = {}
        self.rl_relayed = {}     # proxied_nid => requestor_nid
        self.rl_requestors = {}  # requestor_nid => [proxied_nid]
        self.cl_avail_relays = {}      # relay_nid => [proxied_nid]
        self.cl_proxied = {}     # proxied_nid => relay_nid
        # self.seen_nodes = set() if is_relay else None
        # self.siblings = set()  # this handles NAT/subnet as well as firewall
        # this gets incremented before being used, so we actually start from 0 not -1
        self.version = -1

    def send_message(self, recp_nid, msg_h, current_time):
        # is it a new connection, or an existing but not yet active connection?
        if recp_nid in self.channels_in:
            yield Send, IN, recp_nid, self._next_version(), msg_h
        elif recp_nid in self.channels_out:
            if recp_nid not in self.queues:
                yield Send, OUT, recp_nid, self._next_version(), msg_h
            else:
                self.queues[recp_nid].append(msg_h)
        elif recp_nid in self.queues:
            self.queues[recp_nid].append(msg_h)
        elif recp_nid in self.cl_proxied:
            relay_nid = self.cl_proxied[recp_nid]
            yield RelaySend, (IN if relay_nid in self.channels_in else OUT), relay_nid, recp_nid, msg_h
        else:
            yield Connect, recp_nid
            if self._needs_ping(recp_nid, current_time):
                yield Ping, OUT, recp_nid, self._next_version()
            self.channels_out.add(recp_nid)
            self.last_seen[recp_nid] = current_time
            self.queues.setdefault(recp_nid, []).append(msg_h)

    def sig_disconnect_received(self, sender_nid):
        if sender_nid in self.channels_in:
            self.channels_in.remove(sender_nid)
        elif sender_nid in self.channels_out:
            self.channels_out.remove(sender_nid)
            yield Disconnect, sender_nid
        else:
            return
        if sender_nid in self.cl_avail_relays:
            yield self._handle_relay_down(sender_nid)
        else:
            yield RELAY_CHECKS(self, sender_nid)
        yield NODEDOWN(self, sender_nid)

    def ping_received(self, on_sock, sender_nid, version, current_time):
        self.last_seen[sender_nid] = current_time
        if on_sock == IN and sender_nid not in self.channels_in:
            self.channels_in.add(sender_nid)
            if self.is_relay:
                yield RelaySigNew, IN, sender_nid
            elif sender_nid in self.cl_proxied:
                relay_nid = self.cl_proxied.pop(sender_nid)
                yield RelayNvm, (IN if relay_nid in self.channels_in else OUT), relay_nid, sender_nid
        if on_sock == OUT and sender_nid not in self.channels_out:
            self.channels_out.add(sender_nid)
            if self.is_relay:
                yield RelaySigNew, OUT, sender_nid
        inout = (IN if sender_nid in self.channels_in else OUT)
        if self._needs_ping(sender_nid, current_time):
            yield Ping, inout, sender_nid, self._next_version()
        if sender_nid in self.queues:
            for msg_h in self.queues.pop(sender_nid):
                yield Send, inout, sender_nid, self._next_version(), msg_h
        else:
            if not (version > self.versions.get(sender_nid, 0)):
                # version has been reset--he has restarted, so emulate a node-down-node-back-up event pair:
                self.versions[sender_nid] = version
                assert sender_nid not in self.queues
                if sender_nid in self.cl_avail_relays:
                    self._handle_relay_down(sender_nid)
                yield NodeDown, sender_nid
        self.versions[sender_nid] = version

    def message_received(self, on_sock, sender_nid, version, msg_body_bytes, current_time):
        yield self.ping_received(on_sock, sender_nid, version, current_time)
        if msg_body_bytes:
            yield Receive, sender_nid, msg_body_bytes

    def heartbeat(self, current_time):
        t_gone = current_time - self.heartbeat_max_silence
        for nid in (self.channels_in | self.channels_out):
            if self.last_seen[nid] < t_gone:
                if nid in self.channels_out:
                    self.channels_out.remove(nid)
                    yield Disconnect, nid
                else:
                    self.channels_in.remove(nid)
                RELAY_CHECKS(self, nid)
                if self.is_relay or not self.cl_avail_relays:
                    yield NODEDOWN(self, nid)
                elif nid in self.cl_avail_relays:
                    self._handle_relay_down(nid)
                    yield NODEDOWN(self, nid)
                else:
                    relay_nid = random.choice(self.cl_avail_relays.keys())
                    self.cl_avail_relays[relay_nid].add(nid)
                    self.cl_proxied[nid] = relay_nid
                    yield RelayConnect, (IN if relay_nid in self.channels_in else OUT), relay_nid, nid
            else:
                if self._needs_ping(nid, current_time):
                    yield Ping, (IN if nid in self.channels_in else OUT), nid, self._next_version()

    def new_relay_received(self, nid):
        self.cl_avail_relays[nid] = set()

    def relay_connect_received(self, requestor_nid, proxied_nid):
        if proxied_nid in self.channels_in:
            self.rl_relayed.setdefault(proxied_nid, set()).add(requestor_nid)
            yield RelaySigConnected, IN, requestor_nid, proxied_nid
        else:
            yield RelaySigNodeDown, IN, requestor_nid, proxied_nid

    def relay_connected_received(self, proxied_nid):
        relay_nid = self.cl_proxied[proxied_nid]
        for msg_h in self.queues.pop(proxied_nid):
            yield RelaySend, (IN if relay_nid in self.channels_in else OUT), relay_nid, proxied_nid, msg_h

    def relay_nodedown_received(self, relay_nid, proxied_nid):
        if proxied_nid in self.cl_avail_relays.get(relay_nid, set()):
            self.cl_avail_relays[relay_nid].remove(proxied_nid)
            del self.cl_proxied[proxied_nid]
            yield NODEDOWN(self, proxied_nid)

    def relay_send_received(self, requestor_nid, actual_recipient_nid, actual_msg_bytes):
        if actual_recipient_nid not in self.rl_requestors.get(requestor_nid, set()):
            self.rl_requestors.setdefault(requestor_nid, set()).add(actual_recipient_nid)
            self.rl_relayed.setdefault(actual_recipient_nid, set()).add(requestor_nid)
        yield RelayForward, IN, actual_recipient_nid, requestor_nid, actual_msg_bytes

    def relay_forwarded_received(self, actual_sender_nid, actual_msg_bytes):
        yield Receive, actual_sender_nid, actual_msg_bytes

    def relay_nvm_received(self, sender_nid, proxied_nid):
        self.rl_relayed.get(proxied_nid, set()).discard(sender_nid)
        self.rl_requestors.get(sender_nid, set()).discard(proxied_nid)

    def ensure_connected(self, nid, current_time):
        # open fresh channel
        if nid not in self.channels_in and nid not in self.channels_out and nid not in self.cl_proxied:
            self.last_seen[nid] = current_time
            self.channels_out.add(nid)
            self.queues[nid] = []
            yield Connect, nid
            yield Ping, OUT, nid, self._next_version()

    def shutdown(self):
        for nid in (self.channels_in | self.channels_out):
            yield SigDisconnect, (IN if nid in self.channels_in else OUT), nid
            yield FLUSH(self, nid)
        self.channels_in = self.channels_out = None

    # private:

    def _handle_relay_down(self, relay_nid):
        for proxied_nid in self.cl_avail_relays.pop(relay_nid):
            del self.cl_proxied[proxied_nid]
            yield NODEDOWN(self, proxied_nid)

    def _needs_ping(self, nid, t):
        ret = self.last_sent.get(nid, 0) <= t - self.heartbeat_interval
        if ret:
            self.last_sent[nid] = t
        return ret

    def _next_version(self):
        self.version += 1
        return self.version

    def __repr__(self):
        return "HubLogic()"
