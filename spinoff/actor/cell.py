# coding: utf-8
from __future__ import print_function

import abc
import sys
import traceback
import warnings
import weakref
from collections import deque
from itertools import count

import gevent
import gevent.event
import gevent.queue

from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, ErrorIgnored, Error
from spinoff.actor.exceptions import NameConflict, LookupFailed, Unhandled, CreateFailed, UnhandledTermination, BadSupervision, WrappingException
from spinoff.actor.props import Props
from spinoff.actor.ref import Ref
from spinoff.actor.supervision import Decision, Ignore, Restart, Stop, Default, Escalate
from spinoff.actor.uri import Uri
from spinoff.util.logging import logstring, dbg, fail
from spinoff.util.pattern_matching import ANY
from spinoff.util.pattern_matching import OR


class _BaseCell(object):
    __metaclass__ = abc.ABCMeta

    _children = {}  # XXX: should be a read-only dict
    child_name_gen = None

    @abc.abstractproperty
    def root(self):
        raise NotImplementedError

    @abc.abstractproperty
    def ref(self):
        raise NotImplementedError

    @abc.abstractproperty
    def uri(self):
        raise NotImplementedError

    def spawn(self, factory, name=None):
        """Spawns an actor using the given `factory` with the specified `name`.

        Returns an immediately usable `Ref` to the newly created actor, regardless of the location of the new actor, or
        when the actual spawning will take place.
        """
        if name and '/' in name:  # pragma: no cover
            raise TypeError("Actor names cannot contain slashes")
        if not self._children:
            self._children = {}
        uri = self.uri / name if name else None
        if name:
            if name.startswith('$'):
                raise ValueError("Unable to spawn actor at path %s; name cannot start with '$', it is reserved for auto-generated names" % (uri.path,))
            if name in self._children:
                raise NameConflict("Unable to spawn actor at path %s; actor %r already sits there" % (uri.path, self._children[name]))
        if not uri:
            name = self._generate_name(factory)
            uri = self.uri / name
        assert name not in self._children  # XXX: ordering??
        child = self._children[name] = Cell(parent=self.ref, factory=factory, uri=uri, node=self.node).ref
        return child

    @abc.abstractmethod
    def receive(self, message):
        pass

    def _generate_name(self, factory):
        # TODO: the factory should provide that as a property
        basename = factory.__name__.lower() if isinstance(factory, type) else factory.cls.__name__
        if not self.child_name_gen:
            self.child_name_gen = ('$%d' % i for i in count(1))
        return basename.lower() + self.child_name_gen.next()

    @property
    def children(self):
        return self._children.values()

    def _child_gone(self, child):
        self._children.pop(child.uri.name, None)

    def get_child(self, name):
        if not (name and isinstance(name, str)):
            raise TypeError("get_child takes a non-emtpy string")  # pragma: no cover
        return self._children.get(name, None)

    def lookup_cell(self, uri):
        """Looks up a local actor by its location relative to this actor."""
        steps = uri.steps
        if steps[0] == '':
            found = self.root
            steps.popleft()
        else:
            found = self
        for step in steps:
            assert step != ''
            found = found.get_child(step)
            if not found:
                break
            found = found._cell
        return found

    def lookup_ref(self, uri):
        if not isinstance(uri, (Uri, str)):
            raise TypeError("%s.lookup_ref expects a str or Uri" % type(self).__name__)  # pragma: no cover
        uri = uri if isinstance(uri, Uri) else Uri.parse(uri)
        assert not uri.node or uri.node == self.uri.node
        cell = self.lookup_cell(uri)
        if not cell:
            raise LookupFailed("Look-up of local actor failed: %s" % (uri,))
        return cell.ref


class Cell(_BaseCell):  # TODO: inherit from Greenlet?
    uri = None
    node = None
    impl = None
    proc = None
    stash = None
    stopped = False

    inbox = None

    _ref = None
    child_name_gen = None

    watchers = None
    watchees = None

    def __init__(self, parent, factory, uri, node):
        if not callable(factory):  # pragma: no cover
            raise TypeError("Provide a callable (such as a class, function or Props) as the factory of the new actor")
        self.factory = factory
        self.parent = parent
        self.uri = uri
        self.node = node
        self.queue = gevent.queue.Queue()
        self.inbox = deque()
        self.worker = gevent.spawn(self.work)

    @logstring(u'←')
    def receive(self, message):
        self.queue.put(message)

    @logstring(u'↻')
    def work(self):
        def _restart():
            # dbg(u"► ↻")
            self.shutdown()
            self.worker = gevent.spawn(self.work)
            gevent.getcurrent().kill()

        def _stop():
            # dbg("STOP")
            self.shutdown()
            self.destroy()
            gevent.getcurrent().kill()

        def _resume_children():
            for x in self.children:
                x.send('_resume')

        # dbg(u"►►")
        try:
            self.impl = self.construct()
        except:
            self.report()
            stash = deque()
            while True:
                m = self.queue.get()
                if m in ('_resume', '_stop'):
                    self.queue.queue.extendleft(stash)
                    _stop()
                elif m == '_restart':
                    self.queue.queue.extendleft(stash)
                    _restart()
                else:
                    stash.appendleft(m)
        else:
            processing, has_ever_wanted_a_message = (True, False) if self.impl.run else (False, True)
            error = suspended = stopping = restarting = False
            while True:
                # dbg("processing: %r, error: %r, suspended: %r, stash size: %d, active: %r" % (processing, error, suspended, len(self.stash), has_ever_wanted_a_message))
                # consume the queue, handle system messages, and collect letters to the inbox
                if suspended or processing or error or stopping or not self.inbox:
                    self.queue.peek()
                should_restart = False
                suspend_or_resume = None
                while True:
                    try:
                        m = self.queue.get_nowait()
                    except gevent.queue.Empty:
                        break
                    if m == '__done':
                        has_ever_wanted_a_message = True
                        processing = False
                        if stopping:
                            if self.proc:
                                self.proc.kill()
                            _stop()
                        elif restarting:
                            should_restart = True
                    elif m == ('__error', ANY, ANY):
                        _, exc, tb = m
                        error = True
                        processing = False
                        if stopping:
                            Events.log(ErrorIgnored(self.ref, exc, tb))
                            if self.proc:
                                self.proc.kill()
                            _stop()
                        elif restarting:
                            should_restart = True
                        else:
                            self._suspend_children()
                            self.report((exc, tb))
                    elif m == '_stop':
                        if not processing or not has_ever_wanted_a_message:
                            if self.proc:
                                self.proc.kill()
                            _stop()
                        else:
                            stopping = True
                    elif m == '_restart':
                        if not processing or not has_ever_wanted_a_message:
                            should_restart = True
                        else:
                            restarting = True
                    elif m in ('_suspend', '_resume'):
                        suspend_or_resume = m
                    elif m == '_ignore':
                        assert error, "unexpected _ignore received"
                        if self.impl.run:
                            _stop()
                        else:
                            error = False
                    elif m == ('_watched', ANY):
                        self._watched(m[1])
                    elif m == ('_unwatched', ANY):
                        self.watchers.discard(m[1])
                    else:
                        if m == ('_node_down', ANY):
                            _, node = m
                            self.inbox.extend(('terminated', x) for x in (self.watchees or []) if x.uri.node == node)
                        else:
                            self.inbox.append(m)
                if should_restart:
                    _restart()
                elif suspend_or_resume:
                    suspended_new = (suspend_or_resume == '_suspend')
                    if suspended != suspended_new:
                        # dbg(u"||" if suspended_new else u"► ↻")
                        suspended = suspended_new
                        if suspended:
                            self._suspend_children()
                        else:
                            _resume_children()
                # process the normal letters (i.e. the regular, non-system/non-special messages)
                while not processing and not suspended and not error and not stopping and self.queue.empty() and self.inbox:
                    m = self.inbox.popleft()
                    if m == ('_error', ANY, ANY, ANY):  # error handling is viewed as user-land logic
                        _, sender, exc, tb = m
                        self.catch_exc(self.supervise, sender, exc, tb)
                        break
                    if m == ('terminated', ANY):
                        _, actor = m
                        if actor in self.watchees:
                            self.watchees.remove(actor)
                            self._unwatch(actor, silent=True)
                    elif m == ('_child_terminated', ANY):
                        self._child_gone(m[1])
                        break
                    if self.impl.receive:
                        processing = True
                        self.proc = gevent.spawn(self.catch_exc, self.catch_unhandled, self.impl.receive, m)
                        gevent.idle()
                    elif self.impl.run:
                        assert self.ch.balance == -1
                        if self.get_pt == m:
                            processing = True
                            self.ch.put(m)
                            self.inbox.extendleft(reversed(self.stash))
                            self.stash.clear()
                        else:
                            self.stash.append(m)
                    else:
                        self.catch_exc(self.unhandled, m)

    def _suspend_children(self):
        for x in self.children:
            x.send('_suspend')

    def catch_exc(self, fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception:
            self.queue.put(('__error', sys.exc_info()[1], sys.exc_info()[2]))
        else:
            self.queue.put('__done')

    def catch_unhandled(self, fn, m):
        try:
            fn(m)
        except Unhandled:
            self.unhandled(m)

    # proc

    def get(self, *patterns):
        self.get_pt = OR(*patterns)
        self.queue.put('__done')
        return self.ch.get()

    def flush(self):
        while self.stash:
            self.unhandled(self.stash.popleft())

    # TODO: haven't figured out yet how to cleanly fit an implementation of this to the existing state machine
    # def escalate(self):
    #     _, exc, tb = sys.exc_info()
    #     if not (exc and tb):
    #         raise InvalidEscalation("Process.escalate must be called in an exception context")
    #     self.report((exc, tb))
    #     self._resumed = gevent.event.Event()
    #     self._resumed.wait()

    # birth & death

    def construct(self):
        factory = self.factory
        try:
            impl = factory()
        except Exception:
            raise CreateFailed("Constructing actor failed", factory)
        impl._parent = self.parent
        impl._set_cell(self)
        if hasattr(impl, 'pre_start'):
            pre_start = impl.pre_start
            args, kwargs = impl.args, impl.kwargs
            try:
                pre_start(*args, **kwargs)
            except Exception:
                raise CreateFailed("Actor failed to start", impl)
        if impl.run:
            self.ch = gevent.queue.Channel()
            if impl.receive:
                raise TypeError("actor should implement only run() or receive() but not both")
            self.proc = gevent.spawn(self.wrap_run, impl.run)
            self.stash = deque()
        return impl

    def wrap_run(self, fn):
        try:
            ret = fn(*self.impl.args, **self.impl.kwargs)
        except Exception:
            self.queue.put(('__error', sys.exc_info()[1], sys.exc_info()[2]))
        else:
            if ret is not None:
                warnings.warn("Process.run should not return anything--it's ignored")
            self.queue.put('__done')
            self.queue.put('_stop')

    def shutdown(self, term_msg='_stop'):
        if hasattr(self.impl, 'post_stop'):
            try:
                self.impl.post_stop()
            except Exception:
                Events.log(ErrorIgnored(self.ref, sys.exc_info()[1], sys.exc_info()[2]))
        while self.watchees:
            self._unwatch(self.watchees.pop())
        for child in self.children:
            child << term_msg
        stash = deque()
        while self.children:
            m = self.queue.get()
            if m == ('_child_terminated', ANY):
                self._child_gone(m[1])
            else:
                stash.appendleft(m)
        self.queue.queue.extendleft(stash)

    def destroy(self):
        ref = self.ref  # grab the ref before we stop, otherwise ref() returns a dead ref
        self.stopped = True
        self.queue.queue.extendleft(reversed(self.inbox))
        while True:
            try:
                m = self.queue.get_nowait()
            except gevent.queue.Empty:
                break
            if m == ('_watched', ANY):
                self._watched(m[1])
            elif m == ('_error', ANY, ANY, ANY):
                _, sender, exc, tb = m
                Events.log(ErrorIgnored(sender, exc, tb))
            elif m == ('__error', ANY, ANY):
                _, exc, tb = m
                Events.log(ErrorIgnored(ref, exc, tb))
            elif not (m == ('terminated', ANY) or m == ('_unwatched', ANY) or m == ('_node_down', ANY) or m == '_restart' or m == '_stop' or m == '_suspend' or m == '_resume' or m == '__done'):
                Events.log(DeadLetter(ref, m))
        self.parent.send(('_child_terminated', ref))
        for watcher in (self.watchers or []):
            watcher << ('terminated', ref)
        self.impl = self.inbox = self.queue = ref._cell = self.parent = None

    # unhandled

    def unhandled(self, m):
        if ('terminated', ANY) == m:
            raise UnhandledTermination(watcher=self.ref, watchee=m[1])
        else:
            Events.log(UnhandledMessage(self.ref, m))

    # supervision

    @logstring("SUP")
    def supervise(self, child, exc, tb):
        # dbg(u"%r ← %r" % (exc, child))
        if child not in self.children:
            Events.log(ErrorIgnored(child, exc, tb))
            return
        supervise = getattr(self.impl, 'supervise', None)
        if supervise:
            decision = supervise(exc)
        if not supervise or decision == Default:
            decision = default_supervise(exc)
        dbg(u"%r → %r" % (decision, child))
        if not isinstance(decision, Decision):
            raise BadSupervision("Bad supervisor decision: %s" % (decision,), exc, tb)
        # TODO: make these always async?
        if decision == Ignore:
            child << '_ignore'
        elif decision == Restart(ANY, ANY):
            child << '_restart'
        elif decision == Stop:
            child << '_stop'
        else:
            raise exc, None, tb

    @logstring("report")
    def report(self, exc_and_tb=None):
        if not exc_and_tb:
            _, exc, tb = sys.exc_info()
        else:
            exc, tb = exc_and_tb
        if not isinstance(exc, UnhandledTermination):
            if isinstance(tb, str):
                exc_fmt = tb
            else:
                exc_fmt = ''.join(traceback.format_exception(type(exc), exc, tb))
                if isinstance(exc, WrappingException):
                    inner_exc_fm = traceback.format_exception(type(exc.cause), exc.cause, exc.tb)
                    inner_exc_fm = ''.join('      ' + line for line in inner_exc_fm)
                    exc_fmt += "-------\n" + inner_exc_fm
            fail('\n\n', exc_fmt)
        else:
            fail("Died because a watched actor (%r) died" % (exc.watchee,))
        Events.log(Error(self.ref, exc, tb)),
        self.parent << ('_error', self.ref, exc, tb)

    # death watch

    def watch(self, actor, *actors, **kwargs):
        actors = (actor,) + actors
        actors = [self.spawn(x, **kwargs) if isinstance(x, (type, Props)) else x for x in actors]
        for other in actors:
            node = self.uri.node
            if other != self.ref:
                assert not other.is_local if other.uri.node and other.uri.node != node else True, (other.uri.node, node)
                if not self.watchees:
                    self.watchees = set()
                if other not in self.watchees:
                    self.watchees.add(other)
                    if not other.is_local:
                        self.node.watch_node(other.uri.node, self.ref)
                    other << ('_watched', self.ref)
        return actors[0] if len(actors) == 1 else actors

    def unwatch(self, actor, *actors):
        actors = (actor,) + actors
        if self.watchees:
            for actor in actors:
                try:
                    self.watchees.remove(actor)
                except KeyError:
                    pass
                else:
                    self._unwatch(actor)

    def _unwatch(self, other, silent=False):
        if not silent:
            other << ('_unwatched', self.ref)
        if not other.is_local:
            self.node.unwatch_node(other.uri.node, report_to=self.ref)

    def _watched(self, other):
        if not self.watchers:
            self.watchers = set()
        self.watchers.add(other)

    def _unwatched(self, other):
        if self.watchers:
            self.watchers.discard(other)

    # misc

    @property
    def ref(self):
        if self.stopped:
            return Ref(cell=None, uri=self.uri)
        if not self._ref or not self._ref():
            ref = Ref(self, self.uri)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()

    @property
    def root(self):
        from spinoff.actor.guardian import Guardian
        return self.parent if isinstance(self.parent, Guardian) else self.parent._cell.root

    # logging

    # def logstate(self):  # pragma: no cover
    #     return {'--\\': self.shutting_down, '+': self.stopped, 'N': not self.started, '_': self.suspended, '?': self.tainted, 'X': self.processing_messages, }

    # def logcomment(self):  # pragma: no cover
    #     if not self.queue.empty() or self.inbox:
    #         def g():
    #             for i, msg in enumerate(chain(self.inbox, [' ... '], self.queue.queue)):
    #                 yield msg if isinstance(msg, str) else repr(msg)
    #                 if i == 2:
    #                     yield '...'
    #                     return
    #         return '<<<[%s]' % (', '.join(g()),)
    #     else:
    #         return ''

    def __repr__(self):
        return "<cell:%s@%s>" % (type(self.impl).__name__ if self.impl else (self.factory.__name__ if isinstance(self.factory, type) else self.factory.cls.__name__), self.uri.path,)


def default_supervise(exc):
    if isinstance(exc, CreateFailed):
        return Stop
    elif isinstance(exc, AssertionError):
        return Escalate
    elif isinstance(exc, Exception):
        return Restart
    else:
        assert False, "don't know how to supervise this exception"
    #     return Escalate  # TODO: needed for BaseException
