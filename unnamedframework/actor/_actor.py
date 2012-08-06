from __future__ import print_function

import sys
import types
import warnings
from functools import wraps

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, QueueUnderflow, returnValue, maybeDeferred, _DefGen_Return, CancelledError, succeed
from twisted.python import log
from twisted.python.failure import Failure

from unnamedframework.util.pattern_matching import match, ANY, IGNORE
from unnamedframework.util._defer import inlineCallbacks
from unnamedframework.util.async import combine
from unnamedframework.util.python import combomethod, enumrange


__all__ = [
    'Process', 'Actor', 'process', 'actor', 'NoRoute', 'RoutingException', 'InterfaceException',
    'ActorsAsService', 'NotRunning', 'AlreadyStopped', 'AlreadyRunning', 'UnhandledMessage',
    'ActorRunner', 'NOT_STARTED', 'RUNNING', 'PAUSED', 'STOPPED', ]


EMPTY = object()


class NoRoute(Exception):
    pass


class RoutingException(Exception):
    pass


class InterfaceException(Exception):
    pass


NOT_STARTED, RUNNING, PAUSED, STOPPED = enumrange('NOT_STARTED', 'RUNNING', 'PAUSED', 'STOPPED')


class Actor(object):
    parent = property(lambda self: self._parent)

    is_running = property(lambda self: self._state is RUNNING)
    is_alive = property(lambda self: self._state < STOPPED)
    is_paused = property(lambda self: self._state is PAUSED)

    _state = NOT_STARTED
    _parent = None
    _out = None
    _ref = None

    def __init__(self):
        self._children = []
        self._pending = []
        self._d = Deferred()

    @combomethod
    def _spawn(cls_or_self, *args, **kwargs):
        if not isinstance(cls_or_self, Actor):
            cls = cls_or_self
            ret = cls(*args, **kwargs)
            ret.start()
            return ret
        else:
            return cls_or_self._spawn_child(*args, **kwargs)

    @combomethod
    def spawn(cls_or_self, *args, **kwargs):
        return cls_or_self._spawn(*args, **kwargs).ref

    def _spawn_child(self, actor_cls, *args, **kwargs):
        if isinstance(actor_cls, (types.FunctionType, types.MethodType)):
            actor_cls = process(actor_cls)

        if isinstance(actor_cls, type):
            child = actor_cls(*args, **kwargs)
        else:
            child = actor_cls
        child._parent = self.ref
        child.start()
        self._children.append(child)
        child._d.addBoth(lambda _: self._children.remove(child))
        return child

    _before_start = lambda _: None

    def start(self):
        self.resume()
        try:
            self._wrap_errors(self._before_start)
        except Exception:
            self.stop()

    def _report_error(self, exc_and_traceback):
        """Reports an error to the supervisor of this actor."""
        self.parent.send(('error', self.ref, exc_and_traceback))

    def _wrap_errors(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            self._report_error((e, sys.exc_info()[2]))
            raise

    def send(self, message):
        warnings.warn("Actor.send should not be used")
        self._send(message)

    def _send(self, message):
        if self._state is RUNNING:
            def _receive():
                try:
                    return self.receive(message)
                except UnhandledMessage:
                    is_match, traceback = match(('error', IGNORE(ANY), (IGNORE(ANY), ANY)), message)
                    if is_match:
                        formatted_traceback = (traceback
                                               if isinstance(traceback, basestring) else
                                               traceback.format_tb(traceback))
                        warnings.warn("Unhandled error:\n%s" % formatted_traceback)

            try:
                ret = self._wrap_errors(_receive)
            except Exception:
                pass
            else:
                if isinstance(ret, types.GeneratorType):
                    raise RuntimeError("Actor.receive returned a generator: yield inside Actor.receive?")
        elif self._state is not STOPPED:
            self._pending.append(message)

    def receive(self, message):
        print("Actor %s received %s" % (self, message))

    def pause(self):
        if self._state is RUNNING:
            self._state = PAUSED
            for child in self._children:
                child.pause()

    def resume(self):
        if self._state is RUNNING:
            raise AlreadyRunning("Actor already running")
        if self._state is STOPPED:
            raise AlreadyStopped("Actor has been stopped")
        self._state = RUNNING

        for child in self._children:
            if not child.is_running:
                child.resume()

        if self._pending:
            for pending_message in self._pending:
                self._send(pending_message)
            self._pending = []

    _on_stop = lambda _: None

    def stop(self, silent=False):
        if self._state is STOPPED:
            return
        if self._state is RUNNING:
            self.pause()

        for child in self._children:
            child.stop(silent=True)

        try:
            self._wrap_errors(self._on_stop)
        except Exception:
            pass

        self._state = STOPPED

        if not silent:
            self.parent.send(('stopped', self.ref))
            self._d.callback(None)

    def connect(self, to=None):
        assert not self._out, '%s vs %s' % (self._out, to)
        self._out = to

    def put(self, message):
        """Puts a `message` into one of the `outbox`es of this actor.

        If the specified `outbox` has not been previously connected to anywhere (see `Actor.connect`), a
        `NoRoute` will be raised, i.e. outgoing messages cannot be queued locally and must immediately be delivered
        to an inbox of another component and be queued there (if/as needed).

        Returns a `Deferred` which will be fired when the messages has been delivered to all connected components.

        """
        if not self._out:
            raise NoRoute("Actor %s has no outgoing connection" % repr(self))

        self._out.send(message)


class Process(Actor):
    """An actor that contains a Python generator/coroutine with added support for pausing, resuming and stopping.

    Currently only supports coroutines `yield`ing Twisted `Deferred` objects.

    Internally uses `twisted.internet.defer.inlineCallbacks` and thus all coroutines support all `@inlineCallbacks`
    features such as `returnValue`.

    """

    _fn = None
    _gen = None
    _paused_result = None
    _current_d = None
    _on_hold_d = None
    _cancelling_hold_d = False

    def __init__(self, *args, **kwargs):
        super(Process, self).__init__()

        @wraps(self.run)
        def wrap():
            gen = self.run(*args, **kwargs)
            if not isinstance(gen, types.GeneratorType):
                if gen is not None:
                    warnings.warn("process returned a value that was not None")
                yield None
                returnValue(None)
            self._gen = gen
            fire_current_d = self._fire_current_d
            prev_result = None
            try:
                while True:
                    if not isinstance(prev_result, Exception):
                        x = gen.send(prev_result)
                    else:
                        x = gen.throw(prev_result)
                    if isinstance(x, Deferred):
                        d = Deferred()
                        x.addBoth(fire_current_d, d)
                        self._on_hold_d = x
                        x = d
                    try:
                        prev_result = yield x
                    except BaseException as e:
                        prev_result = e
            except StopIteration:
                # by exiting the while loop, and thus the function, inlineCallbacks will in turn get a StopIteration
                # from us.
                pass
            except _DefGen_Return:
                warnings.warn("returnValue inside an process")
                # StopIteration "raised" implicitly
        self._fn = inlineCallbacks(wrap)

        self._waiting = None
        self._inbox = []

        self._run_args = []
        self._run_kwargs = {}

    def start(self):
        super(Process, self).start()

        d = maybeDeferred(self._fn)

        @d.addBoth
        def finally_(result):
            if isinstance(result, Failure):
                self._report_error((result.value, result.tb or result.getTraceback()))
            self.stop()

    def run(self):
        pass

    def _fire_current_d(self, result, d):
        self._on_hold_d = None
        if self._state is RUNNING and not self._cancelling_hold_d:
            if isinstance(result, Failure):
                d.errback(result)
            else:
                d.callback(result)
        else:
            self._current_d = d
            self._paused_result = result

    def receive(self, message):
        if self._waiting:
            found = EMPTY
            if self._waiting[0] is None:
                found = message
            elif found is EMPTY:
                m, values = match(self._waiting[0], message, flatten=False)
                if m:
                    found = values
            if found is not EMPTY:
                d = self._waiting[1]
                self._waiting = None
                d.callback(found)
                return
        self._inbox.append(message)

    def deliver(self, message):
        warnings.warn("Process.deliver has been deprecated in favor of Process.send", DeprecationWarning)
        return self.send(message)

    def get(self, filter=None):
        # TODO: research possibility of optimising selective receives on long queues;  the queue could be a linked list,
        # and a dict could be used where keys are the generated receive filters, and values are pointers in the queue.

        if self._inbox:
            if filter is None:
                return self._inbox.pop(0)
            for msg in self._inbox:
                m, values = match(filter, msg, flatten=False)
                if m:
                    self._inbox.remove(msg)
                    return succeed(values)

        d = Deferred(lambda d: setattr(self, '_waiting', None))
        if self._waiting:
            raise QueueUnderflow()
        self._waiting = (filter, d)
        return d

    def resume(self):
        super(Process, self).resume()

        if self._current_d:
            if isinstance(self._paused_result, Failure):
                self._current_d.errback(self._paused_result)
            else:
                self._current_d.callback(self._paused_result)
            self._current_d = self._paused_result = None

    def _on_stop(self):
        if self._on_hold_d:
            try:
                self._cancelling_hold_d = True
                try:
                    self._on_hold_d.cancel()
                finally:
                    self._cancelling_hold_d = False
                assert isinstance(self._paused_result.value, CancelledError)
                self._paused_result = None
            except Exception:
                pass

        if self._gen:
            try:
                self._gen.close()
            except _DefGen_Return:
                warnings.warn("returnValue inside an process")
            except Exception as e:
                self._report_error((e, sys.exc_info()[2]))

        if self._state is PAUSED and isinstance(self._paused_result, Failure):
            warnings.warn("Pending exception in paused process")

    def debug_state(self, name=None):
        for message, _ in self._inbox.pending:
            print('*** \t%s' % message)

    def as_service(self):
        warnings.warn("Process.as_service is deprecated, use `twistd runactor -a path.to.ActorClass` instead", DeprecationWarning)
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

    def __init__(self, actor_cls):
        self._actor_cls = actor_cls
        self._actor = None

    def startService(self):
        actor_path = self._actor_path = '%s.%s' % (self._actor_cls.__module__, self._actor_cls.__name__)

        log.msg("running: %s" % actor_path)

        def start_actor():
            try:
                self._actor = self._actor_cls()
                self._actor._parent = self
                self._actor.start()
            except Exception:
                sys.stderr.write("failed to start: %s\n" % actor_path)
                Failure().printTraceback(file=sys.stderr)
                return

        reactor.callLater(0.0, start_actor)

    def send(self, message):
        if message[0] == 'error':
            assert message[1] == self._actor
            sys.stderr.write("failed: %s\n" % self._actor_path)

            if not isinstance(message[2][1], basestring):
                raise message[2][0], None, message[2][1]
            else:
                print(message[2][1], file=sys.stderr)
        elif message[0] == 'done':
            assert message[1] == self._actor
            sys.stderr.write("finished: %s\n" % self._actor_path)
        else:
            sys.stderr.write("received message: %s\n" % repr(message))

    def stopService(self):
        sys.stderr.write("exiting...\n")
        if self._actor and self._actor.is_alive:
            self._actor.stop()


def actor(fn):
    class ret(Actor):
        receive = fn
    ret.__name__ = fn.__name__
    ret.__module__ = fn.__module__
    return ret


def process(fn):
    class ret(Process):
        run = fn
    ret.__name__ = fn.__name__
    ret.__module__ = fn.__module__
    return ret


Actor.def_before_start = classmethod(lambda cls, fn: setattr(cls, '_before_start', fn))
Actor.def_on_stop = classmethod(lambda cls, fn: setattr(cls, '_on_stop', fn))


class AlreadyRunning(Exception):
    pass


class NotRunning(Exception):
    pass


class AlreadyStopped(Exception):
    pass


class UnhandledMessage(Exception):
    pass
