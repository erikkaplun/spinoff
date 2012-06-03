from twisted.internet.defer import inlineCallbacks, Deferred


class MicroProcess(object):

    stopped = property(lambda self: self._stopped)
    running = property(lambda self: self._running)

    _stopped = False
    _running = False

    _fn = None
    _gen = None
    _paused_result = None
    _current_d = None

    def __init__(self, fn):
        def wrap():
            gen = self._gen = fn()
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
        return self._fn()

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
            self._gen.throw(CoroutineStopped())
        except StopIteration:
            pass
        else:
            raise CoroutineRefusedToStop("Coroutine was expected to exit but did not")


def microprocess(fn):
    def ret():
        return MicroProcess(fn)
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
