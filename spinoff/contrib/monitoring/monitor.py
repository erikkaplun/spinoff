from twisted.internet import reactor

from spinoff.actor import Actor
from spinoff.actor.exceptions import Unhandled
from spinoff.util.async import after
from spinoff.util.logging import dbg
from spinoff.util.pattern_matching import ANY, IN
from .http import run_server


__all__ = []

# no need for this to be higher--termination messages will arrive sooner anyway if the monitor is not available:
CLIENT_RECONNECT_INTERVAL = 30.0


class Monitor(Actor):
    def pre_start(self, reactor=reactor):
        self.reactor = reactor

        self.log = []
        self.up = {}  # client => (<up|down>, <last_seen>)
        self.state = {}  # client => state

        self.server = run_server(self.ref)

        self.msg_receive_started = reactor.seconds()
        self.num_msgs_received = 0

    def receive(self, msg):
        t = self.reactor.seconds()

        self.num_msgs_received += 1
        dt = t - self.msg_receive_started
        if dt >= 1.0:
            dbg("%d msg/s" % (float(self.num_msgs_received) / dt))
            self.msg_receive_started = t
            self.num_msgs_received = 0
        self.num_msgs_received += 1

        # dbg(t, msg)

        def logappend(*args):
            pass
            # self.log.append((t,) + args)
            # if len(self.log) > 50000:
                # dbg("trimming log")
            #     self.log = self.log[10000:]

        # Monitoring

        if ('connect', ANY) == msg:
            _, sender = msg
            dbg("connect from", sender)
            sender << 'ack'

        elif ('up', ANY) == msg:
            _, sender = msg
            self.watch(sender)

            logappend('up', str(sender.uri))
            self.up[sender] = ('up', t)

        elif ('terminated', IN(self.up)) == msg:
            _, sender = msg

            logappend('down', str(sender.uri))
            self.up[sender] = ('down', t)

        elif ('state', ANY, ANY) == msg:
            _, sender, state = msg
            if sender in self.up:
                self.up[sender] = (self.up[sender][0], t)
            else:
                logappend('up', str(sender.uri))
                self.up[sender] = ('up', t)
            logappend('state', str(sender.uri), state)
            self.state[sender] = state

        elif ('log', ANY, ANY) == msg:
            _, sender, logmsg = msg
            logappend('log', str(sender.uri), logmsg)

        #

        elif ('get-up', ANY, ANY) == msg:
            _, d, filter = msg
            msg[1].callback([(str(k.uri), v) for k, v in self.up.items() if k == filter])

        elif ('get-state', ANY, ANY) == msg:
            _, d, filter = msg
            msg[1].callback([(str(k.uri), v) for k, v in self.state.items() if k == filter])

        elif ('get-log', ANY, ANY) == msg:
            _, d, filter = msg
            msg[1].callback([x for x in self.log if x == filter])

        elif ('get-all', ANY, ANY) == msg:
            _, d, filter = msg
            data = {}
            for client in self.up:
                uri = str(client.uri)
                if uri == filter:
                    status, seen = self.up[client]
                    state = self.state[client] if client in self.state else None
                    data[uri] = (status, seen, state)
            msg[1].callback([(uri, status, seen, state) for uri, (status, seen, state) in data.items()])

        else:
            raise Unhandled

    def post_stop(self):
        yield self.server.stopListening()

    def __repr__(self):
        return 'Monitor'


class MonitorClient(Actor):
    """Sets up a client to a Monitor actor.

    The main purpose of this is to avoid accumulating and the eventual deadlettering of monitoring messages.
    The client will simply discard any messages until it is sure the Monitor is online and available.

    """
    _instances = {}

    @classmethod
    def get(cls, monitor, node):
        if monitor not in cls._instances:
            cls._instances[monitor] = node.spawn(cls.using(monitor))
        return cls._instances[monitor]

    def __init__(self, monitor):
        self.monitor = monitor

        self.connected = False
        self.connecting = None

    def receive(self, msg):
        if 'ack' == msg:
            if not self.connected:
                self.connected = True
                self.connecting.cancel()
                self.connecting = None

        elif ('terminated', self.monitor) == msg:
            self.connected = False
            self.connect(delayed=True)

        elif self.connected:
            self.monitor << msg

        elif not self.connecting:
            self.connect()

        else:
            pass

    def connect(self, delayed=False):
        if self.connecting and not self.connecting.called:
            self.connecting.cancel()

        self.connecting = reactor.callLater(CLIENT_RECONNECT_INTERVAL, self.connect)

        def do_connect():
            self.watch(self.monitor)
            assert self.monitor in self._Actor__cell.watchees
            self.monitor << ('connect', self.ref)

        if delayed:
            after(1.0).do(do_connect)
        else:
            do_connect()

    def __repr__(self):
        return "<client of %r (%s)>" % (self.monitor, id(self))
