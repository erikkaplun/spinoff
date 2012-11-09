from spinoff.actor.process import Process
from spinoff.util.async import sleep


class LoopingCaller(Process):
    def run(self, interval, fn, *args, **kwargs):
        while True:
            yield sleep(interval)
            yield fn(*args, **kwargs)
