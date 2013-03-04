import gevent

from spinoff.actor.process import Process
from spinoff.util.logging import dbg


class ExampleProcess(Process):
    def run(self):
        while True:
            dbg("sleeping...", gevent.getcurrent())
            gevent.sleep(1.0)
