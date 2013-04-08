from __future__ import print_function

from twisted.python.failure import Failure
from twisted.internet.defer import Deferred, CancelledError
from twisted.internet import reactor, task


__all__ = ['Timeout', 'with_timeout', 'CancelledError', 'sleep']


def sleep(seconds=0, reactor=reactor):
    """A simple helper for asynchronously sleeping a certain amount of time.

    Standard usage:
        sleep(1.0).addCallback(on_wakeup)

    inlineCallbacks usage:
        yield sleep(1.0)

    """
    return task.deferLater(reactor, seconds, lambda: None)


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
    if timeout is None or not isinstance(d, Deferred):
        return d

    ret = Deferred(canceller=lambda _: (
        d.cancel(),
        timeout_d.cancel(),
    ))

    timeout_d = sleep(timeout, reactor)
    timeout_d.addCallback(lambda _: (
        d.cancel(),
        ret.errback(Failure(Timeout())) if not ret.called else None,
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


def if_(condition, then, else_=lambda: None):
    return then() if condition else else_()
