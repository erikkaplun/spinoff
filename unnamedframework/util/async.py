from __future__ import print_function

import traceback
import sys
from functools import wraps
from types import UnboundMethodType

from twisted.python.failure import Failure
from twisted.internet.defer import inlineCallbacks, Deferred, CancelledError, DeferredList
from twisted.internet import reactor, task


__all__ = ['Timeout', 'sleep', 'after', 'call_when_idle', 'cancel_all_idle_calls', 'exec_async', 'if_', 'with_timeout', 'combine', 'CancelledError']


def sleep(seconds, reactor=reactor):
    """A simple helper for asynchronously sleeping a certain amount of time.

    Standard usage:
        sleep(1.0).addCallback(on_wakeup)

    inlineCallbacks usage:
        yield sleep(1.0)

    """
    return task.deferLater(reactor, seconds, lambda: None)


def after(seconds, reactor=reactor):
    """Readability sugar for `sleep`.

    Examples:

        after(2.0).do(lambda: print("Printed 2 seconds later"))

        after(2.0).do(some_fn, 'arg1', some='kwarg')

    """
    return _AfterWrap(sleep(seconds, reactor))


from collections import deque
_idle_calls = deque()
_processing_idle_calls = False


def _process_idle_calls():
    global _processing_idle_calls

    _processing_idle_calls = True
    try:
        while _idle_calls:
            fn, args, kwargs = _idle_calls.popleft()
            try:
                fn(*args, **kwargs)
            except Exception:
                print("*** PANIC: error while processing idle calls", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
    finally:
        _processing_idle_calls = False


def call_when_idle(fn, *args, **kwargs):
    if not _processing_idle_calls:
        reactor.callLater(0, _process_idle_calls)
    _idle_calls.append((fn, args, kwargs))


def call_when_idle_unless_already(fn, *args, **kwargs):
    if (fn, args, kwargs) not in _idle_calls:
        call_when_idle(fn, *args, **kwargs)


def cancel_all_idle_calls():
    print("IDLECALL: canelling calls:", [x[0] for x in _idle_calls], file=sys.stderr)
    _idle_calls.clear()


class _AfterWrap(object):
    def __init__(self, d):
        self.d = d

    def do(self, fn, *args, **kwargs):
        return self.d.addCallback(lambda _: fn(*args, **kwargs))


exec_async = lambda f: inlineCallbacks(f)()


def if_(condition, then, else_=None):
    if condition:
        return then()
    elif else_:
        return else_()


class Timeout(Exception):
    pass


def with_timeout(timeout, d, reactor=reactor):
    """Returns a `Deferred` that is in all respects equivalent to `d`, e.g. when `cancel()` is called on it `Deferred`,
    the wrapped `Deferred` will also be cancelled; however, a `Timeout` will be fired after the `timeout` number of
    seconds if `d` has not fired by that time.

    When a `Timeout` is raised, `d` will be cancelled. It is up to the caller to worry about how `d` handles
    cancellation, i.e. whether it has full/true support for cancelling, or does cancelling it just prevent its callbacks
    from being fired but doesn't cancel the underlying operation.

    """
    if timeout is None:
        return d

    ret = Deferred(canceller=lambda _: (
        d.cancel(),
        timeout_d.cancel(),
        ))

    timeout_d = sleep(timeout, reactor)
    timeout_d.addCallback(lambda _: (
        d.cancel(),
        ret.errback(Failure(Timeout())),
        ))

    timeout_d.addErrback(lambda f: f.trap(CancelledError))

    d.addCallback(lambda result: (
        timeout_d.cancel(),
        ret.callback(result),
        ))

    d.addErrback(lambda f: (
        if_(not f.check(CancelledError), lambda: (
            timeout_d.cancel(),
            ret.errback(f),
            )),
        ))

    return ret


def combine(ds):
    return DeferredList(ds, consumeErrors=True, fireOnOneErrback=True)


class EventBuffer(object):

    _TWISTED_REACTOR = reactor

    def __init__(self, fn, args=[], kwargs={}, milliseconds=1000, reactor=None):
        self._milliseconds = milliseconds
        self._last_event = None
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._reactor = reactor or self._TWISTED_REACTOR

    def call(self, *args, **kwargs):
        t = self._reactor.seconds()
        if self._last_event is None or t - self._last_event >= self._milliseconds:
            self._last_event = t
            _args, _kwargs = [], {}
            if self._args or args:
                _args.extend(self._args)
                _args.extend(args)
            if self._kwargs or kwargs:
                _kwargs.update(self._kwargs)
                _kwargs.update(kwargs)
            self._fn(*_args, **_kwargs)


def with_heartbeat(interval, reactor=reactor):
    if callable(interval):
        return with_heartbeat(1.0)
    else:
        def dec(fn):
            @wraps(fn)
            @inlineCallbacks
            def ret(self, *args, **kwargs):
                cls = type(self)

                heartbeater_num = None
                num_heartbeaters = None

                if not hasattr(cls, '_coroutines'):
                    assert hasattr(self, 'send_heartbeat')
                    all_methods = [getattr(cls, name).im_func for name in cls.__dict__
                                   if isinstance(getattr(cls, name), UnboundMethodType)]
                    cls._coroutines = [method for method in all_methods if hasattr(method, '_is_heartbeater')]

                if heartbeater_num is None:
                    num_heartbeaters = len(cls._coroutines)
                    heartbeater_num = cls._coroutines.index(ret)

                if not hasattr(self, '_heartbeat_cycle'):
                    self._heartbeat_cycle = 0
                if not hasattr(self, '_first_heartbeat_sent'):
                    self._first_heartbeat_sent = False

                def single_heartbeat():
                    self._heartbeat_cycle += 1
                    if self._heartbeat_cycle == num_heartbeaters:
                        self.send_heartbeat()
                        self._heartbeat_cycle = 0

                if not self._first_heartbeat_sent:
                    self._first_heartbeat_sent = True
                    self.send_heartbeat()

                heartbeat_active = False
                coroutine_running = True
                last_heartbeat = [reactor.seconds()] * num_heartbeaters

                @exec_async
                def send_heartbeat():
                    while coroutine_running:
                        yield sleep(float(interval) / 10.0, reactor=reactor)
                        if coroutine_running and heartbeat_active and reactor.seconds() - last_heartbeat[heartbeater_num] >= interval:
                            last_heartbeat[heartbeater_num] = reactor.seconds()
                            single_heartbeat()

                try:
                    for d in fn(self, *args, **kwargs):
                        heartbeat_active = True
                        yield d
                        heartbeat_active = False
                finally:
                    coroutine_running = False

            ret._is_heartbeater = True
            return ret
        return dec
