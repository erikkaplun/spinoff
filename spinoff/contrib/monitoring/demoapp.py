from __future__ import print_function

import random

from spinoff.actor import Actor
from spinoff.actor._actor import lookup
from spinoff.actor.process import Process
from spinoff.actor.supervision import Stop
from spinoff.contrib.monitoring.monitor import Monitor
from spinoff.util.async import sleep, after
from spinoff.util.logging import err
from spinoff.util.pattern_matching import ANY
from spinoff.contrib.monitoring.monitor import MonitorClient


__all__ = []


class DemoApp(Actor):
    def supervise(self, _):
        return Stop

    def pre_start(self, monitor=None, count=1000):
        if not monitor:
            monitor = self.monitor = self.spawn(Monitor, name='monitor')
        else:
            self.monitor = monitor = lookup(monitor) if isinstance(monitor, basestring) else monitor
        NUM = count
        for i in range(NUM):
            self.watch(DemoClient.using(monitor=monitor), name='client%s' % str(i + 1).rjust(len(str(NUM)), '0'))

    def receive(self, msg):
        if ('terminated', ANY) == msg:
            _, client = msg
            after(10.0).do(lambda: (
                self.watch(DemoClient.using(monitor=self.monitor), name=client.uri.name),
            )).onerror(err)


class DemoClient(Process):
    def run(self, monitor):
        monitor = MonitorClient.get(monitor)

        yield sleep(random.random() * 2.0)

        def report(*what):
            monitor << what

        monitor << ('up', self.ref)

        while True:
            for state in ['preparing', 'processing', 'cleaning up']:
                monitor << ('state', self.ref, state)
                yield sleep(3.0 + random.random())
                for action in ['hmm', 'ahaaa', 'got it']:
                    yield report('log', self.ref, action)

            if random.random() < 0:
                return
            else:
                monitor << ('state', self.ref, 'waiting')
                yield sleep(.3)
