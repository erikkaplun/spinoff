# coding: utf8
from __future__ import print_function, absolute_import

import re
import struct
from cPickle import dumps
from collections import deque

from twisted.internet.defer import inlineCallbacks
from txzmq import ZmqEndpoint

from spinoff.actor import _actor
from spinoff.actor._actor import _VALID_NODEID_RE
from spinoff.actor.events import Events, DeadLetter
from spinoff.util.async import sleep
from spinoff.util.logging import logstring, log
from spinoff.util.pattern_matching import ANY, IN


# TODO: use shorter messages outside of testing
# MUST be a single char
PING = b'0'
DISCONNECT = b'1'

PING_VERSION_FORMAT = '!I'

_VALID_ADDR_RE = re.compile('tcp://%s' % (_VALID_NODEID_RE.pattern,))
_PROTO_ADDR_RE = re.compile('(tcp://)(%s)' % (_VALID_NODEID_RE.pattern,))


class Connection(object):
    watching_actors = None
    queue = None

    def __init__(self, owner, addr, sock, our_addr, time, known_remote_version):
        self.owner = owner
        self.addr = addr
        self.sock = sock = sock
        self.our_addr = our_addr
        # even if this is a fresh connection, there's no need to set seen to None--in fact it would be illogical, as by
        # connecting, we're making an assumption that it exists; and pragmatically speaking, the heartbeat algorithm
        # also wouldn't be able to deal with seen=None without adding an extra attribute.
        self.seen = time

        self.watched_actors = set()
        self.queue = deque()

        self.known_remote_version = known_remote_version

        sock.addEndpoints([ZmqEndpoint('connect', addr)])

    @property
    def is_active(self):
        return self.queue is None

    def send(self, ref, msg):
        if self.queue is not None:
            self.queue.append((ref, msg))
        else:
            if self.sock:
                self._do_send(dumps((ref.uri.path, msg), protocol=2))
            else:
                Events.log(DeadLetter(ref, msg))

    def established(self, remote_version):
        log()
        self.known_remote_version = remote_version
        self._flush_queue()
        del self.queue

    @inlineCallbacks
    def close(self, disconnect_other=True):
        log()

        if disconnect_other:
            self.sock.sendMultipart((self.our_addr, DISCONNECT))
        if not _actor.TESTING:
            # have to avoid this during testing, and it's not needed anyway;
            # during testing, since everything is running in the same thread with no remoting (by default)
            # this will unnecessarily give control back to the test which means stuff will happen in the middle
            # of closing.
            # XXX: this actually should be possible regardless of the above comment
            yield sleep(0)

        self._kill_queue()
        self._emit_termination_messages()
        self.sock.shutdown()
        self.sock = self.owner = None

    @logstring(u" ❤⇝")
    def heartbeat(self):
        self._do_send(PING + struct.pack(PING_VERSION_FORMAT, self.owner.version))
        self.owner.version += 1

    @logstring(u"⇝")
    def _do_send(self, msg):
        self.sock.sendMultipart((self.our_addr, msg))

    def _flush_queue(self):
        q, self.queue = self.queue, None
        while q:
            ref, msg = q.popleft()
            self._do_send(dumps((ref.uri.path, msg), protocol=2))

    def _kill_queue(self):
        q, self.queue = self.queue, None
        while q:
            ref, msg = q.popleft()
            if (IN(['_watched', '_unwatched', 'terminated']), ANY) != msg:
                Events.log(DeadLetter(ref, msg))

    def watch(self, report_to):
        if not self.watching_actors:
            self.watching_actors = set()
        self.watching_actors.add(report_to)

    def unwatch(self, report_to):
        if self.watching_actors:
            self.watching_actors.discard(report_to)

    def _emit_termination_messages(self):
        if self.watching_actors:
            w, self.watching_actors = self.watching_actors, None
            for report_to in w:
                report_to << ('_node_down', self.addr[len('tcp://'):])

    def __del__(self):
        if hasattr(self, 'sock') and self.sock:
            self.sock.shutdown()
            del self.sock

    def __repr__(self):
        return (('<connection:%s->%s>' % (self.owner.nodeid, self.addr[len('tcp://'):]))
                if self.owner else
                '<connection:dead>')
