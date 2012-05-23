from twisted.python.failure import Failure
from twisted.internet.defer import inlineCallbacks, Deferred, TimeoutError, CancelledError, DeferredList
from twisted.internet import reactor, task


__all__ = ['TimeoutError', 'sleep', 'exec_async', 'if_', 'with_timeout', 'combine']


def sleep(seconds, reactor=reactor):
    """A simple helper for asynchronously sleeping a certain amount of time.

    Standard usage:
        sleep(1.0).addCallback(on_wakeup)

    inlineCallbacks usage:
        yield sleep(1.0)

    """
    return task.deferLater(reactor, seconds, lambda: None)


exec_async = lambda f: inlineCallbacks(f)()


def if_(condition, then, else_=None):
    if condition:
        return then()
    elif else_:
        return else_()


def with_timeout(timeout, d, reactor=reactor):
    """Returns a `Deferred` that is in all respects equivalent to `d`, e.g. when `cancel()` is called on it `Deferred`,
    the wrapped `Deferred` will also be cancelled; however, a `TimeoutError` will be fired after the `timeout` number of
    seconds if `d` has not fired by that time.

    When a `TimeoutError` is raised, `d` will be cancelled. It is up to the caller to worry about how `d` handles
    cancellation, i.e. whether it has full/true support for cancelling, or does cancelling it just prevent its callbacks
    from being fired but doesn't cancel the underlying operation.

    """
    ret = Deferred(canceller=lambda _: (
        d.cancel(),
        timeout_d.cancel(),
        ))

    timeout_d = sleep(timeout, reactor)
    timeout_d.addCallback(lambda _: (
        d.cancel(),
        ret.errback(Failure(TimeoutError())),
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


def combine(*ds):
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
