# coding: utf-8
from __future__ import print_function

import abc
import inspect
import sys
import traceback
import warnings

from twisted.internet.defer import Deferred, CancelledError
from txcoroutine import coroutine

from spinoff.actor import Actor, ActorType, CreateFailed
from spinoff.actor.events import HighWaterMarkReached, Events
from spinoff.actor.exceptions import InvalidEscalation
from spinoff.util.async import call_when_idle
from spinoff.util.pattern_matching import OR
from spinoff.util.logging import logstring, flaw, panic


class ProcessType(ActorType):
    def __new__(self, name, bases, dict_):
        """Verifies that the run method is a generator, and wraps it with `txcoroutine.coroutine`."""
        ret = super(ProcessType, self).__new__(self, name, bases, dict_)

        path_of_new_class = dict_['__module__'] + '.' + name

        if path_of_new_class != 'spinoff.actor.process.Process':
            if not inspect.isgeneratorfunction(ret.run):
                raise TypeError("Process.run must return a generator")
            ret.run = coroutine(ret.run)

        return ret


# This class does a lot of two things:
#  1) use double-underscore prefxied members for stronger privacy--this is normally ugly but in this case warranted to
#     make sure nobody gets tempted to access that stuff.
#  2) access private members of the Actor class because that's the most (memory) efficient way and Process really would
#     be a friend class of Actor if Python had such thing.
class Process(Actor):
    __metaclass__ = ProcessType

    hwm = 10000  # default high water mark

    __get_d = None
    __queue = None
    __pre_start_complete_d = None

    _coroutine = None

    @abc.abstractmethod
    def run(self, *args, **kwargs):  # pragma: no cover
        yield

    def pre_start(self, *args, **kwargs):
        # dbg()
        self.__pre_start_complete_d = Deferred()
        try:
            self._coroutine = self.run(*args, **kwargs)
            # dbg("coroutine created")
            self._coroutine.addCallback(self.__handle_complete)
            if self._coroutine:
                self._coroutine.addErrback(self.__handle_failure)
            # dbg("...waiting for ready...")
            yield self.__pre_start_complete_d
            # dbg(u"✓")
        finally:
            del self.__pre_start_complete_d

    def receive(self, msg):
        # dbg("recv %r" % (msg,))
        if self.__get_d and self.__get_d.wants(msg):
            # dbg("injecting %r" % (msg,))
            self.__get_d.callback(msg)
            # dbg("...injectng %r OK" % (msg,))
        else:
            # dbg("queueing")
            if not self.__queue:
                self.__queue = []
            self.__queue.append(msg)
            l = len(self.__queue)
            if l and l % self.hwm == 0:
                Events.log(HighWaterMarkReached(self.ref, l))

    @logstring("get")
    def get(self, *patterns):
        # dbg("get")
        pattern = OR(*patterns)
        try:
            if self.__pre_start_complete_d:
                # dbg("first get")
                self.__pre_start_complete_d.callback(None)
                self.__pre_start_complete_d = None

            if self.__queue:
                try:
                    ix = self.__queue.index(pattern)
                except ValueError:
                    pass
                else:
                    # dbg("next message from queue")
                    return self.__queue.pop(ix)

            # dbg("ready for message")
            self.__get_d = _PickyDeferred(pattern, canceller=self.__clear_get_d)
            self.__get_d.addCallback(self.__clear_get_d)
            return self.__get_d
        except Exception:  # pragma: no cover
            panic(traceback.format_exc())

    def __clear_get_d(self, result):
        self.__get_d = None
        return result

    def __handle_failure(self, f):
        # dbg("deleting self._coroutine")
        del self._coroutine
        if isinstance(f.value, CancelledError):
            return

        # dbg()
        try:
            if self.__pre_start_complete_d:
                # dbg("failure during start")
                self.__pre_start_complete_d.errback(f)
            else:
                try:
                    f.raiseException()
                except Exception:
                    # XXX: seems like a hack but should be safe;
                    # hard to do it better without convoluting `Actor`
                    self._Actor__cell.tainted = True
                    # dbg("...reporting to parent")
                    self._Actor__cell.report_to_parent()
        except Exception:  # pragma: no cover
            panic("failure in handle_faiure:\n", traceback.format_exc())

    def __handle_complete(self, result):
        # dbg()
        if result:
            warnings.warn("Process.run should not return anything--it's ignored")
        del self._coroutine
        if self.__pre_start_complete_d:
            self.__pre_start_complete_d.callback(None)
        self.stop()

    def __shutdown(self):
        # dbg()
        if self._coroutine:
            self._coroutine.cancel()
        if self.__get_d:
            del self.__get_d

    def flush(self):
        if self.__queue:  # XXX: added without testing
            for message in self.__queue:
                self._Actor__cell._unhandled(message)
            del self.__queue[:]

    @logstring(u"escalate ↑")
    def escalate(self):
        # dbg(repr(sys.exc_info()[1]))
        _, exc, tb = sys.exc_info()
        if not (exc and tb):
            flaw("programming flaw: escalate called outside of exception context")
            raise InvalidEscalation("Process.escalate must be called in an exception context")
        if self.__pre_start_complete_d:
            # if the Process tried to .escalate() before it went into receive-mode, i.e. pre_start was still
            # incomplete, simply turn the escalation into a regular exception reported as CreateFailed; CreateFailed
            # will get the exc and tb from context.
            # (would be nice to say "process" here, but it would be inconsistent with other startup errors in the coroutine)
            raise CreateFailed("Actor failed to start", self)
        ret = Deferred()
        call_when_idle(lambda: (
            self._Actor__cell.report_to_parent((exc, tb)),
            ret.callback(None),
        ))
        return ret

    def __repr__(self):
        return Actor.__repr__(self).replace('<actor-impl:', '<proc-impl:')


class _PickyDeferred(Deferred):
    def __init__(self, pattern, *args, **kwargs):
        Deferred.__init__(self, *args, **kwargs)
        self.pattern = pattern

    def wants(self, result):
        return result == self.pattern

    def callback(self, result):
        assert self.wants(result)
        Deferred.callback(self, result)
