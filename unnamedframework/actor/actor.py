from __future__ import print_function

import sys
import warnings
import types

from twisted.application.service import Service
from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet.defer import Deferred, QueueUnderflow
from unnamedframework.util.async import combine
from zope.interface import Interface, implements
from unnamedframework.util.python import combomethod
from unnamedframework.util.microprocess import MicroProcess, microprocess
import unnamedframework.util.pattern as match


__all__ = ['IActor', 'IProducer', 'IConsumer', 'Actor', 'NoRoute', 'RoutingException', 'InterfaceException', 'ActorsAsService']


EMPTY = object()


class NoRoute(Exception):
    pass


class RoutingException(Exception):
    pass


class InterfaceException(Exception):
    pass


class IProducer(Interface):

    def connect(component):
        """Connects this component to another `component`.

        It is legal to pass in `self` as the value of `component` if needed.

        """


class IConsumer(Interface):

    def send(message):
        """Sends an incoming `message` into one of the `inbox`es of this component.

        Returns a `Deferred` which will be fired when this component has received the `message`.

        """


class IActor(IProducer, IConsumer):
    pass


class Actor(MicroProcess):
    implements(IActor)

    parent = property(lambda self: self._parent)

    def __init__(self, *args, **kwargs):
        super(Actor, self).__init__()
        self._waiting = None
        self._inbox = []
        self._out = None
        self._parent = None
        self._children = []

        self._run_args = []
        self._run_kwargs = {}

    @combomethod
    def spawn(cls_or_self, *args, **kwargs):
        if not isinstance(cls_or_self, Actor):
            cls = cls_or_self
            ret = cls(*args, **kwargs)
            d = ret.start()
            d.addErrback(lambda f: (
                f.printTraceback(sys.stderr),
                ))
            return ret
        else:
            self = cls_or_self
            if 'actor_cls' in kwargs:
                actor_cls = kwargs.pop('actor_cls')
            elif len(args) >= 0:
                actor_cls = args[0]
                args = args[1:]
            else:
                raise TypeError("spawn() requires an actor class to be passed as the "
                                "first argument or actor_cls keyword argument")

            if isinstance(actor_cls, (types.FunctionType, types.MethodType)):
                actor_cls = microprocess(actor_cls)

            def on_result(result):
                if result is not None:
                    warnings.warn("actor returned a value but this value will be lost--"
                                  "send it to the parent explicitly instead")

            child = actor_cls(*args, **kwargs)
            if hasattr(child, '_parent'):
                child._parent = self
            if hasattr(child, 'start'):
                d = child.start()
                self._children.append(child)
                d.addCallback(on_result)
                d.addErrback(lambda f: self.send(('child-failed', child, f.value)))
                d.addBoth(lambda result: (self._children.remove(child), result)[-1])
            return child

    def join(self, other):
        return other.d

    def join_children(self):
        return combine([x.d for x in self._children])

    def send(self, message):
        if self._waiting:
            found = EMPTY
            if self._waiting[0] is None:
                found = message
            elif found is EMPTY:
                m, values = match.match(self._waiting[0], message)
                if m:
                    found = values
            if found is not EMPTY:
                d = self._waiting[1]
                self._waiting = None
                d.callback(found)
                return
        self._inbox.append(message)

    def deliver(self, message):
        warnings.warn("Actor.deliver has been deprecated in favor of Actor.send", DeprecationWarning)
        return self.send(message)

    def connect(self, to=None):
        assert not self._out, '%s vs %s' % (self._out, to)
        self._out = to

    def get(self, filter=None):
        if self._inbox:
            if filter is None:
                return self._inbox.pop(0)
            for msg in self._inbox:
                m, values = match.match(filter, msg)
                if m:
                    return values

        d = Deferred(lambda d: setattr(self, '_waiting', None))
        if self._waiting:
            raise QueueUnderflow()
        self._waiting = (filter, d)
        return d

    def put(self, message):
        """Puts a `message` into one of the `outbox`es of this component.

        If the specified `outbox` has not been previously connected to anywhere (see `Actor.connect`), a
        `NoRoute` will be raised, i.e. outgoing messages cannot be queued locally and must immediately be delivered
        to an inbox of another component and be queued there (if/as needed).

        Returns a `Deferred` which will be fired when the messages has been delivered to all connected components.

        """
        if not self._out:
            raise NoRoute("Actor %s has no outgoing connection" % repr(self))

        self._out.send(message)

    def _on_complete(self):
        # mark this actor as stopped only when all children have been joined
        ret = self.join_children()
        ret.addCallback(lambda result: super(Actor, self)._on_complete())
        return ret

    def pause(self):
        super(Actor, self).pause()
        for child in self._children:
            if child.is_running:
                child.pause()

    def resume(self):
        super(Actor, self).resume()
        for child in self._children:
            assert child.is_paused
            child.resume()

    def stop(self):
        super(Actor, self).stop()
        for child in self._children:
            child.stop()

    def debug_state(self, name=None):
        for message, _ in self._inbox.pending:
            print('*** \t%s' % message)

    def as_service(self):
        warnings.warn("Actor.as_service is deprecated, use `twistd runactor -a path.to.ActorClass` instead", DeprecationWarning)
        return ActorsAsService([self])


class ActorsAsService(Service):

    def __init__(self, actors):
        warnings.warn("ActorsAsService is deprecated, use `twistd runactor -a path.to.ActorClass` instead", DeprecationWarning)
        self._actors = actors

    def startService(self):
        for x in self._actors:
            x.start()

    def stopService(self):
        return combine([d for d in [x.stop() for x in self._actors] if d])


class ActorRunner(Service):

    def __init__(self, actor):
        self._actor = actor

    def startService(self):
        actor_path = '%s.%s' % (type(self._actor).__module__, type(self._actor).__name__)

        log.msg("running: %s" % actor_path)

        try:
            d = self._actor.start()
        except Exception:
            sys.stderr.write("failed to start: %s\n" % actor_path)
            Failure().printTraceback(file=sys.stderr)
            return

        @d.addBoth
        def finally_(result):
            if isinstance(result, Failure):
                sys.stderr.write("failed: %s\n" % actor_path)
                result.printTraceback(file=sys.stderr)
            else:
                sys.stderr.write("finished: %s\n" % actor_path)

            # os.kill(os.getpid(), signal.SIGKILL)

    def stopService(self):
        if self._actor.is_alive:
            self._actor.stop()


def actor(fn):
    class ret(Actor):
        run = fn
    ret.__name__ = fn.__name__
    return ret
