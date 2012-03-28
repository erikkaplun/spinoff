import sys
from contextlib import contextmanager
from functools import wraps

from twisted.internet.task import Clock


def deferred(f):
    """This is almost exactly the same as nose.twistedtools.deferred, except it allows one to have simulated time in
    their test code by passing in a `Clock` instance. That `Clock` instance will be `advance`d as long as there are
    any deferred calls bound to it.

    """
    @wraps(f)
    def ret():
        error = [None]

        clock = Clock()

        d = f(clock)
        @d.addErrback
        def on_error(f):
            error[0] = sys.exc_info()

        while True:
            time_to_wait = max([0] + [call.getTime() - clock.seconds() for call in clock.getDelayedCalls()])
            if time_to_wait == 0:
                break
            else:
                clock.advance(time_to_wait)

        if error[0]:
            exc_info = error[0]
            raise exc_info[0], exc_info[1], exc_info[2]

    return ret


@contextmanager
def assert_not_raises(exc_class=Exception, message=None):
    try:
        yield
    except exc_class as e:
        raise AssertionError(message or "No exception should have been raised but instead %s was raised" % repr(e))
