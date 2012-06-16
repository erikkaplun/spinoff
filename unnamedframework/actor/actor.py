from __future__ import print_function

import sys
import warnings
import types
from functools import wraps

from twisted.application.service import Service
from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet.defer import Deferred, QueueUnderflow, returnValue, maybeDeferred, _DefGen_Return
from unnamedframework.util.async import combine
from zope.interface import Interface, implements
from unnamedframework.util.python import combomethod
import unnamedframework.util.pattern as match

from unnamedframework.util._defer import inlineCallbacks


__all__ = [
    'IActor', 'IProducer', 'IConsumer', 'Actor', 'actor', 'NoRoute', 'RoutingException', 'InterfaceException',
    'ActorsAsService', 'CoroutineStopped', 'CoroutineNotRunning', 'CoroutineAlreadyStopped', 'CoroutineAlreadyRunning',
    'CoroutineRefusedToStop']


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


NOT_STARTED, RUNNING, PAUSED, STOPPED = range(4)


class Actor(object):
    """A Python generator/coroutine wrapped up to support pausing, resuming and stopping.

    Currently only supports coroutines `yield`ing Twisted `Deferred` objects.

    Internally uses `twisted.internet.defer.inlineCallbacks` and thus all coroutines support all `@inlineCallbacks`
    features such as `returnValue`.

    """

    implements(IActor)

    parent = property(lambda self: self._parent)

    is_running = property(lambda self: self._state is RUNNING)
    is_alive = property(lambda self: self._state < STOPPED)
    is_paused = property(lambda self: self._state is PAUSED)

    _state = NOT_STARTED
    _fn = None
    _gen = None
    _paused_result = None
    _current_d = None

    def __init__(self, *args, **kwargs):
        @wraps(self.run)
        def wrap():
            gen = self._gen = self.run(*args, **kwargs)
            if not isinstance(gen, types.GeneratorType):
                yield None
                returnValue(gen)
            fire_current_d = self._fire_current_d
            prev_result = None
            try:
                while True:
                    x = gen.send(prev_result)
                    if isinstance(x, Deferred):
                        d = Deferred()
                        x.addBoth(fire_current_d, d)
                        x = d
                    prev_result = yield x
            except StopIteration:
                # by exiting the while loop, and thus the function, inlineCallbacks will in turn get a StopIteration
                # from us.
                pass
        self._fn = inlineCallbacks(wrap)

        self._waiting = None
        self._inbox = []
        self._out = None
        self._parent = None
        self._children = []

        self._run_args = []
        self._run_kwargs = {}

    def start(self):
        self.resume()
        self.d = maybeDeferred(self._fn)

        @self.d.addBoth
        def finally_(result):
            d = self._on_complete()
            if d and not isinstance(result, Failure):
                return d
            else:
                print("FOO", result)
                return result

        return self.d

    def run(self):
        pass

    def _fire_current_d(self, result, d):
        if self._state is RUNNING:
            if isinstance(result, Failure):
                d.errback(result)
            else:
                d.callback(result)
        else:
            self._current_d = d
            self._paused_result = result

    @combomethod
    def spawn(cls_or_self, *args, **kwargs):
        if not isinstance(cls_or_self, Actor):
            cls = cls_or_self
            ret = cls(*args, **kwargs)
            d = ret.start()
            # d.addErrback(lambda f: (
            #     f.printTraceback(sys.stderr),
            #     f
            #     ))
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
                actor_cls = actor(actor_cls)

            # def on_result(result):
            #     if result is not None:
            #         warnings.warn("actor returned a value but this value will be lost--"
            #                       "send it to the parent explicitly instead")

            child = actor_cls(*args, **kwargs)
            if hasattr(child, '_parent'):
                child._parent = self
            if hasattr(child, 'start'):
                d = child.start()
                self._children.append(child)
                # d.addCallback(on_result)
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
        ret.addCallback(lambda result: setattr(self, '_state', STOPPED))
        return ret

    def pause(self):
        if self._state is not RUNNING:
            raise CoroutineNotRunning()
        self._state = PAUSED
        for child in self._children:
            if child._state is RUNNING:
                child.pause()

    def resume(self):
        if self._state is RUNNING:
            raise CoroutineAlreadyRunning("Microprocess already running")
        if self._state is STOPPED:
            raise CoroutineAlreadyStopped("Microprocess has been stopped")
        self._state = RUNNING
        if self._current_d:
            if isinstance(self._paused_result, Failure):
                self._current_d.errbackback(self._paused_result)
            else:
                self._current_d.callback(self._paused_result)
            self._current_d = self._paused_result = None

        for child in self._children:
            assert child.is_paused
            child.resume()

    def stop(self):
        if self._state is NOT_STARTED:
            raise Exception("Microprocess not started")
        if self._state is STOPPED:
            raise CoroutineAlreadyStopped("Microprocess already stopped")
        if self._state is RUNNING:
            self.pause()
        try:
            if self._gen:
                try:
                    try:
                        self._gen.throw(CoroutineStopped())
                    except CoroutineStopped:
                        raise StopIteration()
                except StopIteration:
                    pass
                except _DefGen_Return as ret:  # XXX: is there a way to let inlineCallbacks handle this for us?
                    self.d.callback(ret.value)
                else:
                    raise CoroutineRefusedToStop("Coroutine was expected to exit but did not")
        finally:
            if self._state is PAUSED and isinstance(self._paused_result, Failure):
                warnings.warn("Pending exception in paused microprocess")
                # self._paused_result.printTraceback()
            self._state = STOPPED

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


class CoroutineStopped(Exception):
    pass


class CoroutineRefusedToStop(Exception):
    pass


class CoroutineAlreadyRunning(Exception):
    pass


class CoroutineNotRunning(Exception):
    pass


class CoroutineAlreadyStopped(Exception):
    pass
