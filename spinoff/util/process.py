import os

from twisted.internet import reactor, protocol
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessTerminated


__all__ = ['spawn_process', 'ProcessFailed']


ALLOWED_TERMINATION_SIGNALS = ('KILL', 'TERM', 'INT')


class ProcessFailed(Exception):
    pass


class spawn_process(protocol.ProcessProtocol, Deferred):
    """Spawns a new subprocess.

    Returns a `Deferred` that is fired when the process ends. If the process ends successfully,
    the `callback` is fired with a list containing the `stdout` of the process, otherwise the
    `errback` is fired with a `ProcessFailed` instance with `args` set to the error code and the
    `stderr` of the process.

    The `Deferred` returned can be `cancel`ed, causing the process to be terminated with the
    specified `terminaion_signal` which defaults to `KILL`.

    Examples:

        try:
            output = yield spawn_process(...)
        except ProcessFailed as e:
            err_code, stderr_output = e.args
            ...
        else:
            # do smth with output


        try:
            output = yield with_timeout(20.0, spawn_process(...))
        except TimeoutError:
            print("Process timed out")
        except ProcessFailed as e:
            # handle failure
        else:
            # do smth with output

    """

    def __init__(self, executable, args=[], env=None, termination_signal='KILL', path=None):
        assert termination_signal in ALLOWED_TERMINATION_SIGNALS
        Deferred.__init__(self, canceller=lambda _: self._kill())
        self._termination_signal = termination_signal
        self._stdout_output = []
        self._stderr_output = []
        self._killed = False

        reactor.spawnProcess(self, executable, args=[os.path.basename(executable)] + args, env=env, path=path)

    def _kill(self):
        self.transport.signalProcess(self._termination_signal)
        self._killed = True

    def processEnded(self, status):
        if not self._killed:
            if isinstance(status.value, ProcessTerminated):
                self.errback(ProcessFailed(status.value.exitCode, self._stderr_output))
            else:
                self.callback(self._stdout_output)

    def outReceived(self, data):
        self._stdout_output.append(data)

    def errReceived(self, data):
        self._stderr_output.append(data)
