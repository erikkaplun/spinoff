import warnings
from types import GeneratorType
from functools import wraps

from twisted.internet.defer import Deferred, _DefGen_Return, returnValue, maybeDeferred
from twisted.python.failure import Failure

# XXX: this is fragile--might break when t.i.d.inlineCallbacks changes
from ._defer import inlineCallbacks


NOT_STARTED, RUNNING, PAUSED, STOPPED = range(4)


class MicroProcess(object):
    """A Python generator/coroutine wrapped up to support pausing, resuming and stopping.

    Currently only supports coroutines `yield`ing Twisted `Deferred` objects.

    Internally uses `twisted.internet.defer.inlineCallbacks` and thus all coroutines support all `@inlineCallbacks`
    features such as `returnValue`.

    """

    _state = NOT_STARTED
    _fn = None
    _gen = None
    _paused_result = None
    _current_d = None

    is_running = property(lambda self: self._state is RUNNING)
    is_alive = property(lambda self: self._state < STOPPED)
    is_paused = property(lambda self: self._state is PAUSED)

    def __init__(self, *args, **kwargs):
        @wraps(self.run)
        def wrap():
            gen = self._gen = self.run(*args, **kwargs)
            if not isinstance(gen, GeneratorType):
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

    def run(self):
        pass

    def _fire_current_d(self, result, d):
        if self.is_running:
            if isinstance(result, Failure):
                d.errback(result)
            else:
                d.callback(result)
        else:
            self._current_d = d
            self._paused_result = result

    def start(self):
        self.resume()
        self.d = maybeDeferred(self._fn)

        @self.d.addBoth
        def finally_(result):
            d = self._on_complete()
            if d and not isinstance(result, Failure):
                ret = Deferred()
                d.addCallback(lambda _: result)  # pass the original result through
                d.chainDeferred(ret)  # ...but other than that wait on the new deferred
                return ret
            else:
                return result

        return self.d

    def _on_complete(self):
        self._state = STOPPED

    def pause(self):
        if self._state is not RUNNING:
            raise CoroutineNotRunning()
        self._state = PAUSED

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


def microprocess(fn):
    class ret(MicroProcess):
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
