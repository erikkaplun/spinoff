from types import GeneratorType
from functools import wraps

from twisted.internet.defer import Deferred, _DefGen_Return, returnValue

# XXX: this is fragile--might break when t.i.d.inlineCallbacks changes
from ._defer import inlineCallbacks


class MicroProcess(object):
    """A Python generator/coroutine wrapped up to support pausing, resuming and stopping.

    Currently only supports coroutines `yield`ing Twisted `Deferred` objects.

    Internally uses `twisted.internet.defer.inlineCallbacks` and thus all coroutines support all `@inlineCallbacks`
    features such as `returnValue`.

    """

    _stopped = False
    _running = False

    _fn = None
    _gen = None
    _paused_result = None
    _current_d = None

    is_running = property(lambda self: self._running)
    is_alive = property(lambda self: not self._stopped)
    is_paused = property(lambda self: not self.is_running and self.is_alive)

    def __init__(self, fn, *args, **kwargs):
        @wraps(fn)
        def wrap():
            gen = self._gen = fn(*args, **kwargs)
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
                        x.addCallback(fire_current_d, d)
                        x = d
                    prev_result = yield x
            except StopIteration:
                pass
        self._fn = inlineCallbacks(wrap)

    def _fire_current_d(self, result, d):
        if self._running:
            d.callback(result)
        else:
            self._current_d = d
            self._paused_result = result

    def start(self):
        self.resume()
        self.d = self._fn()
        self.d.addBoth(self._on_complete)
        return self.d

    def _on_complete(self, result):
        self._running = False
        self._stopped = True
        return result

    def pause(self):
        if not self._running:
            raise CoroutineNotRunning()
        self._running = False

    def resume(self):
        if self._running:
            raise CoroutineAlreadyRunning("Microprocess already running")
        if self._stopped:
            raise CoroutineAlreadyStopped("Microprocess has been stopped")
        self._running = True
        if self._current_d:
            self._current_d.callback(self._paused_result)
            self._current_d = self._paused_result = None

    def stop(self):
        if self._stopped:
            raise CoroutineAlreadyStopped("Microprocess already stopped")
        self._stopped = True
        if self._running:
            self.pause()
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


def microprocess(fn):
    @wraps(fn)
    def ret(*args, **kwargs):
        return MicroProcess(fn, *args, **kwargs)
    ret.__is_microprocess = True
    return ret


def is_microprocess(fn):
    return getattr(fn, '__is_microprocess', False)


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
