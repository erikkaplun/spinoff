from __future__ import print_function

import sys
import random
from types import GeneratorType

from spinoff.util.python import enumrange
from spinoff.util.logging import dbg


(
    Bind, Connect, Disconnect, SigDisconnect, Send, Ping,
    RelaySigNew, RelayConnect, RelaySigConnected, RelaySend, RelayForward, RelaySigNodeDown, RelayNvm,
    Receive, SendFailed, NodeDown, NextBeat
) = enumrange(
    'Bind', 'Connect', 'Disconnect', 'SigDisconnect', 'Send', 'Ping',
    'RelaySigNew', 'RelayConnect', 'RelaySigConnected', 'RelaySend', 'RelayForward', 'RelaySigNodeDown', 'RelayNvm',
    'Receive', 'SendFailed', 'NodeDown', 'NextBeat'
)
IN, OUT = enumrange('IN', 'OUT')
BIG_BANG_T = -sys.maxint


def nid2addr(nid):
    return nid.rsplit('|', 1)[0]


def FLUSH(self, nid):
    for msg_h in self.queues.pop(nid, []):
        yield SendFailed, msg_h


def NODEDOWN(self, nid):
    yield FLUSH(self, nid)
    yield NodeDown, nid
    for x in [self.last_seen, self.last_sent, self.versions]:
        x.pop(nid, None)


def RELAY_NODEDOWN_CHECKS(self, nid):
    for relayee_nid in self.rl_relayers.pop(nid, set()):
        relayees_relayers = self.rl_relayees[relayee_nid]
        relayees_relayers.remove(nid)
        if not relayees_relayers:
            del self.rl_relayees[relayee_nid]
    for relayer_nid in self.rl_relayees.pop(nid, set()):
        relayers_relayees = self.rl_relayers[relayer_nid]
        relayers_relayees.remove(nid)
        if not relayers_relayees:
            del self.rl_relayers[relayer_nid]
        yield RelaySigNodeDown, IN, relayer_nid, nid


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
    def __init__(self, nid, heartbeat_interval, heartbeat_max_silence, is_relay=False):
        self.nid = nid
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_max_silence = heartbeat_max_silence
        self.is_relay = is_relay
        self.channels_in = set()
        self.channels_out = set()
        self.last_seen = {}
        self.last_sent = {}
        self.versions = {}
        self.queues = {}
        self.rl_relayees = {}      # relayee_nid => relayer_nid
        self.rl_relayers = {}      # relayer_nid => [relayee_nid]
        self.cl_avail_relays = {}  # relay_nid => [relayee_nid]
        self.cl_relayees = {}      # relayee_nid => relay_nid
        # this gets incremented before being used, so we actually start from 0 not -1
        self.version = -1

    def start(self):
        port = self.nid.split(':', 1)[1]
        yield Bind, nid2addr('0.0.0.0:' + port)
        yield NextBeat, self.heartbeat_interval

    def send_message(self, rcpt_nid, msg_h, t):
        # is it a new connection, or an existing but not yet active connection?
        if rcpt_nid in self.channels_in:
            yield Send, IN, rcpt_nid, self._next_version(), msg_h
        elif rcpt_nid in self.channels_out:
            if rcpt_nid not in self.queues:
                yield Send, OUT, rcpt_nid, self._next_version(), msg_h
            else:
                self.queues[rcpt_nid].append(msg_h)
        elif rcpt_nid in self.queues:
            self.queues[rcpt_nid].append(msg_h)
        elif rcpt_nid in self.cl_relayees:
            relay_nid = self.cl_relayees[rcpt_nid]
            yield RelaySend, (IN if relay_nid in self.channels_in else OUT), relay_nid, rcpt_nid, msg_h
        else:
            self.last_sent[rcpt_nid] = t
            self.channels_out.add(rcpt_nid)
            self.last_seen[rcpt_nid] = t
            self.queues.setdefault(rcpt_nid, []).append(msg_h)
            yield Connect, nid2addr(rcpt_nid)
            yield Ping, OUT, rcpt_nid, self._next_version()
            if self.is_relay:
                yield RelaySigNew, OUT, rcpt_nid

    def sig_disconnect_received(self, sender_nid):
        if sender_nid in self.channels_in or sender_nid in self.channels_out:
            if sender_nid in self.channels_in:
                self.channels_in.remove(sender_nid)
            if sender_nid in self.channels_out:
                self.channels_out.remove(sender_nid)
                yield Disconnect, nid2addr(sender_nid)
        else:
            return
        if sender_nid in self.cl_avail_relays:
            yield self._handle_relay_down(sender_nid)
        else:
            yield RELAY_NODEDOWN_CHECKS(self, sender_nid)
        yield NODEDOWN(self, sender_nid)

    def ping_received(self, on_sock, sender_nid, version, t):
        if on_sock == OUT and sender_nid not in self.channels_out:
            return
        self.last_seen[sender_nid] = t
        if on_sock == IN and sender_nid not in self.channels_in:
            self.channels_in.add(sender_nid)
            if self.is_relay:
                yield RelaySigNew, IN, sender_nid
            elif sender_nid in self.cl_relayees:
                relay_nid = self.cl_relayees.pop(sender_nid)
                self.cl_avail_relays[relay_nid].remove(sender_nid)
                yield RelayNvm, (IN if relay_nid in self.channels_in else OUT), relay_nid, sender_nid
        inout = (IN if sender_nid in self.channels_in else OUT)
        if self._needs_ping(sender_nid, t):
            yield Ping, inout, sender_nid, self._next_version()
        if sender_nid in self.queues:
            for msg_h in self.queues.pop(sender_nid):
                yield Send, inout, sender_nid, self._next_version(), msg_h
        else:
            if not (version > self.versions.get(sender_nid, -1)):
                # version has been reset--he has restarted, so emulate a node-down-node-back-up event pair:
                self.versions[sender_nid] = version
                assert sender_nid not in self.queues
                if sender_nid in self.cl_avail_relays:
                    self._handle_relay_down(sender_nid)
                yield NodeDown, sender_nid
        self.versions[sender_nid] = version

    def message_received(self, on_sock, sender_nid, version, msg_body_bytes, t):
        yield self.ping_received(on_sock, sender_nid, version, t)
        if msg_body_bytes:
            yield Receive, sender_nid, msg_body_bytes

    def heartbeat(self, t):
        t_gone = t - self.heartbeat_max_silence
        for nid in (self.channels_in | self.channels_out):
            if self.last_seen[nid] <= t_gone:
                if nid in self.channels_out:
                    self.channels_out.remove(nid)
                if nid in self.channels_in:
                    self.channels_in.remove(nid)
                yield Disconnect, nid2addr(nid)
                yield RELAY_NODEDOWN_CHECKS(self, nid)
                if self.is_relay or not self.cl_avail_relays:
                    yield NODEDOWN(self, nid)
                elif nid in self.cl_avail_relays:
                    self._handle_relay_down(nid)
                    yield NODEDOWN(self, nid)
                else:
                    relay_nid = random.choice(self.cl_avail_relays.keys())
                    self.cl_avail_relays[relay_nid].add(nid)
                    self.cl_relayees[nid] = relay_nid
                    yield RelayConnect, (IN if relay_nid in self.channels_in else OUT), relay_nid, nid
            else:
                if self._needs_ping(nid, t):
                    yield Ping, (IN if nid in self.channels_in else OUT), nid, self._next_version()
        yield NextBeat, self.heartbeat_interval

    def new_relay_received(self, nid):
        self.cl_avail_relays[nid] = set()

    def relay_connect_received(self, on_sock, relayer_nid, relayee_nid):
        if on_sock == IN and relayer_nid in self.channels_in or on_sock == OUT and relayer_nid in self.channels_out:
            if relayee_nid in self.channels_in:
                self.rl_relayees.setdefault(relayee_nid, set()).add(relayer_nid)
                self.rl_relayers.setdefault(relayer_nid, set()).add(relayee_nid)
                yield RelaySigConnected, on_sock, relayer_nid, relayee_nid
            else:
                yield RelaySigNodeDown, on_sock, relayer_nid, relayee_nid

    def relay_connected_received(self, relayee_nid):
        if relayee_nid in self.cl_relayees:
            relay_nid = self.cl_relayees[relayee_nid]
            for msg_h in self.queues.pop(relayee_nid):
                yield RelaySend, (IN if relay_nid in self.channels_in else OUT), relay_nid, relayee_nid, msg_h

    def relay_nodedown_received(self, relay_nid, relayee_nid):
        if relayee_nid in self.cl_avail_relays.get(relay_nid, set()):
            self.cl_avail_relays[relay_nid].remove(relayee_nid)
            del self.cl_relayees[relayee_nid]
            yield NODEDOWN(self, relayee_nid)

    def relay_send_received(self, relayer_nid, relayee_nid, relayed_bytes):
        if relayee_nid not in self.channels_in:
            return
        if relayee_nid not in self.rl_relayers.get(relayer_nid, set()):
            self.rl_relayers.setdefault(relayer_nid, set()).add(relayee_nid)
            self.rl_relayees.setdefault(relayee_nid, set()).add(relayer_nid)
        yield RelayForward, IN, relayee_nid, relayer_nid, relayed_bytes

    def relay_forwarded_received(self, actual_sender_nid, relayed_bytes):
        yield Receive, actual_sender_nid, relayed_bytes

    def relay_nvm_received(self, sender_nid, relayee_nid):
        self.rl_relayees.get(relayee_nid, set()).discard(sender_nid)
        self.rl_relayers.get(sender_nid, set()).discard(relayee_nid)

    def ensure_connected(self, nid, t):
        # open fresh channel
        if nid not in self.channels_in and nid not in self.channels_out and nid not in self.cl_relayees:
            self.last_seen[nid] = t
            self.channels_out.add(nid)
            self.queues[nid] = []
            yield Connect, nid2addr(nid)
            self.last_sent[nid] = t
            yield Ping, OUT, nid, self._next_version()
            if self.is_relay:
                yield RelaySigNew, OUT, nid

    def shutdown(self):
        for nid in (self.channels_in | self.channels_out):
            yield SigDisconnect, (IN if nid in self.channels_in else OUT), nid
            yield FLUSH(self, nid)
        self.channels_in = self.channels_out = None

    # private:

    def _handle_relay_down(self, relay_nid):
        for relayee_nid in self.cl_avail_relays.pop(relay_nid):
            del self.cl_relayees[relayee_nid]
            yield NODEDOWN(self, relayee_nid)

    def _needs_ping(self, nid, t):
        ret = self.last_sent.get(nid, BIG_BANG_T) <= t - self.heartbeat_interval / 3.0
        if ret:
            self.last_sent[nid] = t
        return ret

    def _next_version(self):
        self.version += 1
        return self.version

    def __repr__(self):
        return "HubLogic()"


def flatten(gen):
    for x in gen:
        if isinstance(x, GeneratorType):
            for y in flatten(x):
                yield y
        else:
            yield x
