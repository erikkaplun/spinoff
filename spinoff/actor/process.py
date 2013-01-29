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


# This class does a lot of two things:
#  1) use double-underscore prefxied members for stronger privacy--this is normally ugly but in this case warranted to
#     make sure nobody gets tempted to access that stuff.
#  2) access private members of the Actor class because that's the most (memory) efficient way and Process really would
#     be a friend class of Actor if Python had such thing.
class Process(Actor):
    hwm = 10000  # default high water mark

    __waiting_get = None
    __queue = None

    _proc = None

    @abc.abstractmethod
    def run(self, *args, **kwargs):  # pragma: no cover
        yield

    def pre_start(self, *args, **kwargs):
        # dbg()
        self._proc = gevent.spawn(self.run, *args, **kwargs)
        # dbg("coroutine created")
        self._proc.link_value(self.__handle_complete)
        self._proc.link_exception(self.__handle_failure)
        # dbg(u"✓")

    def receive(self, msg):
        # dbg("recv %r" % (msg,))
        if self.__waiting_get and self.__waiting_get.wants(msg):
            # dbg("injecting %r" % (msg,))
            self.__waiting_get.set(msg)
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
        if self.__queue:
            try:
                ix = self.__queue.index(pattern)
            except ValueError:
                pass
            else:
                # dbg("next message from queue")
                return self.__queue.pop(ix)

        # dbg("ready for message")
        w = self.__waiting_get = _PickyAsyncResult(pattern)
        w.rawlink(self.__clear_get_d)
        return self.__waiting_get.get()

    def __clear_get_d(self, _):
        self.__waiting_get = None

    def __handle_failure(self, proc):
        # dbg("deleting self._proc")
        del self._proc
        # XXX: seems like a hack but should be safe;
        # hard to do it better without convoluting `Actor`
        self._Actor__cell.tainted = True
        # dbg("...reporting to parent")
        self._Actor__cell.report((proc.exception, proc.traceback))

    def __handle_complete(self, result):
        # dbg()
        if result:
            warnings.warn("Process.run should not return anything--it's ignored")
        del self._proc
        self.stop()

    def __shutdown(self):
        # dbg()
        if self._proc:
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
        # dbg(repr(sys.exc_info()[1]))
        _, exc, tb = sys.exc_info()
        if not (exc and tb):
            flaw("programming flaw: escalate called outside of exception context")
            raise InvalidEscalation("Process.escalate must be called in an exception context")
        self._Actor__cell.report((exc, tb))
        self._resumed = gevent.event.Event()
        self._resumed.wait()

    def __repr__(self):
        return Actor.__repr__(self).replace('<actor-impl:', '<proc-impl:')


class _PickyAsyncResult(gevent.event.AsyncResult):
    def __init__(self, pattern, *args, **kwargs):
        gevent.event.AsyncResult.__init__(self, *args, **kwargs)
        self.pattern = pattern

    def wants(self, result):
        return result == self.pattern

    def set(self, result):
        assert self.wants(result)
        gevent.event.AsyncResult.set(self, result)
