# coding: utf-8
from __future__ import print_function

import abc
import sys
import traceback
import warnings

import gevent
from spinoff.actor import Actor
from spinoff.actor.events import HighWaterMarkReached, Events
from spinoff.actor.exceptions import InvalidEscalation
from spinoff.util.async import call_when_idle
from spinoff.util.pattern_matching import OR
from spinoff.util.logging import logstring, flaw, panic
from spinoff.util.logging import dbg


# This class does a lot of two things:
#  1) use double-underscore prefxied members for stronger privacy--this is normally ugly but in this case warranted to
#     make sure nobody gets tempted to access that stuff.
#  2) access private members of the Actor class because that's the most (memory) efficient way and Process really would
#     be a friend class of Actor if Python had such thing.
class Process(Actor):
    hwm = 10000  # default high water mark

    __waiting_get = None
    __waiting_get_pattern = None
    __queue = None

    _proc = None

    @abc.abstractmethod
    def run(self, *args, **kwargs):  # pragma: no cover
        pass

    def pre_start(self, *args, **kwargs):
        self._proc = gevent.spawn(self.run, *args, **kwargs)
        self._proc.link_value(self.__handle_complete)
        self._proc.link_exception(self.__handle_failure)

    def receive(self, msg):
        if self.__waiting_get and self.__waiting_get_pattern == msg:
            self.__waiting_get.set(msg)
            self.__waiting_get = None
        else:
            if not self.__queue:
                self.__queue = []
            self.__queue.append(msg)
            l = len(self.__queue)
            if l and l % self.hwm == 0:
                Events.log(HighWaterMarkReached(self.ref, l))

    @logstring("get")
    def get(self, *patterns):
        pattern = OR(*patterns)
        if self.__queue:
            try:
                ix = self.__queue.index(pattern)
            except ValueError:
                pass
            else:
                return self.__queue.pop(ix)
        self.__waiting_get = gevent.event.AsyncResult()
        self.__waiting_get_pattern = pattern
        return self.__waiting_get.get()

    def __handle_failure(self, proc):
        self._proc = None
        self._Actor__cell.report((proc.exception, proc.traceback))

    def __handle_complete(self, result):
        if result.value and not isinstance(result.value, gevent.GreenletExit):
            warnings.warn("Process.run should not return anything--it's ignored")
        self._proc = None
        self.stop()

    def __shutdown(self):
        if self._proc is not None:
            self._proc.kill()
        if self.__waiting_get:
            del self.__waiting_get

    def flush(self):
        if self.__queue:  # XXX: added without testing
            for message in self.__queue:
                self._Actor__cell._unhandled(message)
            del self.__queue[:]

    @logstring(u"escalate ↑")
    def escalate(self):
        _, exc, tb = sys.exc_info()
        if not (exc and tb):
            flaw("programming flaw: escalate called outside of exception context")
            raise InvalidEscalation("Process.escalate must be called in an exception context")
        self._Actor__cell.report((exc, tb))
        self._resumed = gevent.event.Event()
        self._resumed.wait()


class _PickyAsyncResult(gevent.event.AsyncResult):
    def __init__(self, pattern):
        gevent.event.AsyncResult.__init__(self)
        self.pattern = pattern

    def wants(self, result):
        return self.pattern == result

    def set(self, result):
        assert self.wants(result)
        gevent.event.AsyncResult.set(self, result)
