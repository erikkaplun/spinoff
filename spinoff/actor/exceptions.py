import sys
import traceback


class NameConflict(Exception):
    pass


class Unhandled(Exception):
    pass


class UnhandledTermination(Exception):
    def __init__(self, watcher, watchee):
        self.watcher = watcher
        self.watchee = watchee


class WrappingException(Exception):
    def raise_original(self):
        raise self.cause, None, self.tb

    def formatted_original_tb(self):
        return ''.join(traceback.format_exception(self.cause, None, self.tb))


class CreateFailed(WrappingException):
    def __init__(self, message, actor):
        Exception.__init__(self)
        self.tb_fmt = '\n'.join('    ' + line for line in traceback.format_exc().split('\n') if line)
        _, self.cause, self.tb = sys.exc_info()
        self.actor = actor
        self.message = message
        self.args = (message, actor)

    def __repr__(self):
        return 'CreateFailed(%r, %s, %s)' % (self.message, self.actor, repr(self.cause))


class BadSupervision(WrappingException):
    def __init__(self, message, exc, tb):
        WrappingException.__init__(self, message)
        self.cause, self.tb = exc, tb


class LookupFailed(RuntimeError):
    pass


class InvalidEscalation(RuntimeError):
    pass
