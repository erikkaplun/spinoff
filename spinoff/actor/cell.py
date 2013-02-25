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

from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, Error
from spinoff.actor.exceptions import NameConflict, LookupFailed, Unhandled, UnhandledTermination
from spinoff.actor.props import Props
from spinoff.actor.ref import Ref
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
        def _stop():
            # dbg("STOP")
            self.shutdown()
            self.destroy()
            gevent.getcurrent().kill()

        # dbg(u"►►")
        try:
            self.impl = self.construct()
        except Exception:
            self.report()
            _stop()
            return
        processing = True if self.impl.run else False
        stopped = False
        while True:
            # dbg("processing: %r, error: %r, suspended: %r, stash size: %s, active: %r" % (processing, error, suspended, len(self.stash) if self.stash is not None else '-'))
            # consume the queue, handle system messages, and collect letters to the inbox
            if processing or not self.inbox:
                self.queue.peek()
            while True:
                try:
                    m = self.queue.get_nowait()
                except gevent.queue.Empty:
                    break
                # dbg("@ CTRL:", m)
                if m == '__done':
                    processing = False
                    if stopped:
                        m = '_stop'  # fall thru to the _stop/_kill handler
                    else:
                        continue
                if m == ('__error', ANY, ANY):
                    _, exc, tb = m
                    self.report((exc, tb))
                    _stop()
                elif m in ('_kill', '_stop'):
                    if m == '_kill':
                        processing = False
                    if not processing:
                        if self.proc:
                            self.proc.kill()
                        _stop()
                    else:
                        stopped = True
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
            # process the normal letters (i.e. the regular, non-system/non-special messages)
            while not processing and self.queue.empty() and self.inbox:
                m = self.inbox.popleft()
                # dbg("@ NORMAL:", m)
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

    # birth & death

    def construct(self):
        factory = self.factory
        impl = factory()
        impl._parent = self.parent
        impl._set_cell(self)
        if hasattr(impl, 'pre_start'):
            pre_start = impl.pre_start
            args, kwargs = impl.args, impl.kwargs
            pre_start(*args, **kwargs)
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
            except:
                self.report()
        while self.watchees:
            self._unwatch(self.watchees.pop())
        for child in self.children:
            child << term_msg
        stash = deque()
        self.queue.queue.extendleft(reversed(self.inbox))
        self.inbox.clear()
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
        while True:
            try:
                m = self.queue.get_nowait()
            except gevent.queue.Empty:
                break
            if m == ('_watched', ANY):
                self._watched(m[1])
            elif m == ('__error', ANY, ANY):
                _, exc, tb = m
                self.report((exc, tb))
            elif not (m == ('terminated', ANY) or m == ('_unwatched', ANY) or m == ('_node_down', ANY) or m == '_stop' or m == '_kill' or m == '__done'):
                Events.log(DeadLetter(ref, m))
        self.parent.send(('_child_terminated', ref))
        for watcher in (self.watchers or []):
            watcher << ('terminated', ref)
        self.impl = self.inbox = self.queue = ref._cell = self.parent = None

    def unhandled(self, m):
        if ('terminated', ANY) == m:
            raise UnhandledTermination(watcher=self.ref, watchee=m[1])
        else:
            Events.log(UnhandledMessage(self.ref, m))

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
            print(exc_fmt.strip(), file=sys.stderr)
        else:
            fail("Died because a watched actor (%r) died" % (exc.watchee,))
        Events.log(Error(self.ref, exc, tb)),

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
            self.node.unwatch_node(other.uri.node, self.ref)

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
        return "<cell:%s>" % (self.uri.path,)
