# coding: utf-8
from __future__ import print_function

import gevent.event

from spinoff.actor._actor import Actor
from spinoff.actor.ref import _BaseRef
from spinoff.util.async import call_when_idle


class Future(gevent.event.AsyncResult):
    def send(self, message):
        (self.set if not isinstance(message, BaseException) else self.set_exception)(message)


class TempActor(Actor):
    pool = set()

    @classmethod
    def make(cls, context):
        # if not cls.pool:
        d = gevent.event.AsyncResult()
        ret = context.spawn(cls.using(d, cls.pool))
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
        call_when_idle(d.set_exception if isinstance(msg, BaseException) else d.set, msg)


class TypedRef(_BaseRef):
    """Base class for typed references.

    Typed references allow providing of a strongly typed interface to actor refs by exposing (a subset of) the list of
    the messages an actor responds to as methods.

    This class provides the same interface as regular, untyped references. Methods of subclasses of this class should
    be implemented by (directly or indirectly) sending messages to `self.ref`.

    To turn an untyped reference into a typed reference, wrap it with the `TypedRef` subclass.

    Example:

        class Adder(Actor):
            def receive(self, msg):
            if ('add', ANY, ANY) == msg:
                _, operands, r = msg
                r << sum(operands)
            else:
                raise Unhandled

            class Ref(TypedRef):
                def add(self, *operands):
                    deferred, tmp = TempActor.spawn()
                    self.ref << ('add', operands, tmp)
                    return deferred

        adder = Adder.Ref(self.spawn(Adder))
        sum_ = adder.add(1, 2)

    """

    def __init__(self, ref):
        self.ref = ref

    @property
    def is_local(self):
        return self.ref.is_local

    @property
    def is_stopped(self):
        return self.ref.is_stopped

    @property
    def uri(self):
        return self.ref.uri

    def __div__(self, next):
        return self.ref.__div__(next)

    def __lshift__(self, message):
        return self.ref.__lshift__(message)

    def stop(self):
        return self.ref.stop()

    def __repr__(self):
        return repr(self.ref)
