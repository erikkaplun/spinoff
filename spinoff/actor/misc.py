# coding: utf-8
from __future__ import print_function

import gevent.event

from spinoff.actor import Actor
from spinoff.actor.context import get_context


class Future(gevent.event.AsyncResult):
    def send(self, message):
        (self.set if not isinstance(message, BaseException) else self.set_exception)(message)

    __lshift__ = send


class TempActor(Actor):
    pool = set()

    @classmethod
    def make(cls):
        # if not cls.pool:
        d = gevent.event.AsyncResult()
        ret = get_context().spawn(cls.using(d, cls.pool))
        return ret, d
        # TODO:this doesn't work reliably for some reason, otherwise it could be a major performance enhancer,
        # at least for as long as actors are as heavy as they currently are
        # else:
        #     return cls.pool.pop()

    def pre_start(self, d, pool):
        self.d = d
        self.pool = pool

    def receive(self, msg):
        d, self.d = self.d, gevent.event.AsyncResult()
        self.pool.add((self.ref, self.d))
        (d.set_exception if isinstance(msg, BaseException) else d.set)(msg)
