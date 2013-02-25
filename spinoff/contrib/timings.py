from gevent import sleep

from spinoff.actor import Actor


class LoopingCaller(Actor):
    def run(self, interval, fn, *args, **kwargs):
        while True:
            sleep(interval)
            fn(*args, **kwargs)
