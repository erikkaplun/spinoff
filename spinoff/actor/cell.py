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
from gevent import GreenletExit, Greenlet
from gevent.queue import Empty

from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, Error
from spinoff.actor.exceptions import NameConflict, LookupFailed, Unhandled, UnhandledTermination
from spinoff.actor.props import Props
from spinoff.actor.ref import Ref
from spinoff.actor.uri import Uri
from spinoff.util.logging import logstring, dbg, fail
from spinoff.util.pattern_matching import ANY
from spinoff.util.pattern_matching import OR


_NOSENDER = None


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

    def spawn_actor(self, factory, name=None):
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
        child = self._children[name] = Cell.spawn(parent_actor=self.ref, factory=factory, uri=uri, node=self.node).ref
        return child

    @abc.abstractmethod
    def receive(self, message, _sender):
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


class Cell(Greenlet, _BaseCell):
    uri = None
    node = None
    actor = None
    proc = None
    stash = None
    stopped = False

    inbox = None

    _ref = None
    child_name_gen = None

    watchers = None
    watchees = None

    def __init__(self, parent_actor, factory, uri, node):
        Greenlet.__init__(self)
        if not callable(factory):  # pragma: no cover
            raise TypeError("Provide a callable (such as a class, function or Props) as the factory of the new actor")
        self.factory = factory
        self.parent_actor = parent_actor
        self.uri = uri
        self.node = node
        self.queue = gevent.queue.Queue()
        self.inbox = deque()

    @logstring(u'←')
    def receive(self, message, _sender):
        self.queue.put((_sender, message))

    @logstring(u'↻')
    def _run(self):
        def _stop():
            # dbg("STOP")
            self.shutdown()
            self.destroy()
            gevent.getcurrent().kill()

        # dbg(u"►►")
        try:
            self.actor = self.construct()
        except Exception:
            self.report()
            _stop()
            return
        processing = True if self.actor.run else False
        stopped = False
        while True:
            # dbg("processing: %r, error: %r, suspended: %r, stash size: %s, active: %r" % (processing, error, suspended, len(self.stash) if self.stash is not None else '-'))
            # consume the queue, handle system messages, and collect letters to the inbox
            if processing or not self.inbox:
                self.queue.peek()
            while True:
                try:
                    sender, m = self.queue.get_nowait()
                except gevent.queue.Empty:
                    break
                # dbg("@ CTRL:", m)
                if m == '__done':
                    processing = False
                    if stopped:
                        m = '_stop'  # fall thru to the _stop/_kill handler
                    else:
                        continue
                elif m == '__undone':
                    processing = True
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
                    self._unwatched(m[1])
                else:
                    if m == ('_node_down', ANY):
                        _, node = m
                        self.inbox.extend((_NOSENDER, ('terminated', x)) for x in (self.watchees or []) if x.uri.node == node)
                    else:
                        self.inbox.append((sender, m))
            # process the normal letters (i.e. the regular, non-system/non-special messages)
            while not processing and self.queue.empty() and self.inbox:
                sender, m = self.inbox.popleft()
                # dbg("@ NORMAL:", m)
                if m == ('terminated', ANY):
                    _, actor = m
                    if self.watchees and actor in self.watchees:
                        self.watchees.remove(actor)
                        self._unwatch(actor, silent=True)
                    else:
                        continue
                elif m == ('_child_terminated', ANY):
                    self._child_gone(m[1])
                    break
                self.actor.sender = sender
                if self.actor.receive:
                    processing = True
                    self.proc = gevent.spawn(self.catch_exc, self.catch_unhandled, self.actor.receive, m, sender)
                    self.proc._cell = self
                elif self.actor.run:
                    assert self.ch.balance == -1
                    if self.get_pt == m:
                        processing = True
                        self.ch.put(m)
                        self.inbox.extendleft(reversed(self.stash))
                        self.stash.clear()
                    else:
                        self.stash.append((sender, m))
                else:
                    self.catch_exc(self.unhandled, m, sender)

    def catch_exc(self, fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception:
            self.queue.put((_NOSENDER, ('__error', sys.exc_info()[1], sys.exc_info()[2])))
        else:
            self.queue.put((_NOSENDER, '__done'))

    def catch_unhandled(self, fn, m, sender):
        try:
            fn(m)
        except Unhandled:
            self.unhandled(m, sender)

    # proc

    def get(self, pattern=ANY, timeout=None):
        assert timeout is None or isinstance(timeout, (int, float))
        self.get_pt = pattern
        self.queue.put((_NOSENDER, '__done'))
        try:
            return self.ch.get(timeout=timeout)
        except Empty:
            self.queue.put((_NOSENDER, '__undone'))
            raise

    def get_nowait(self, pattern):
        return self.get(pattern, timeout=0.0)

    def flush(self):
        while self.stash:
            sender, m = self.stash.popleft()
            self.unhandled(m, sender)

    # birth & death

    def construct(self):
        factory = self.factory
        actor = factory()
        actor._parent = self.parent_actor
        actor._set_cell(self)
        if hasattr(actor, 'pre_start'):
            pre_start = actor.pre_start
            args, kwargs = actor.args, actor.kwargs
            pre_start(*args, **kwargs)
        if actor.run:
            self.ch = gevent.queue.Channel()
            if actor.receive:
                raise TypeError("actor should implement only run() or receive() but not both")
            self.proc = gevent.spawn(self.wrap_run, actor.run)
            self.proc._cell = self
            self.stash = deque()
        return actor

    def wrap_run(self, fn):
        try:
            ret = fn(*self.actor.args, **self.actor.kwargs)
        except GreenletExit:
            ret = None
        except:
            self.queue.put((_NOSENDER, ('__error', sys.exc_info()[1], sys.exc_info()[2])))
            return
        if ret is not None:
            warnings.warn("Process.run should not return anything--it's ignored")
        self.queue.put((_NOSENDER, '__done'))
        self.queue.put((_NOSENDER, '_stop'))

    def shutdown(self, term_msg='_stop'):
        if hasattr(self.actor, 'post_stop'):
            try:
                self.actor.post_stop()
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
            sender, m = self.queue.get()
            if m == ('_child_terminated', ANY):
                self._child_gone(m[1])
            else:
                stash.appendleft((sender, m))
        self.queue.queue.extendleft(stash)

    def destroy(self):
        if self._ref and self._ref():
            ref = self._ref()  # grab the ref before we stop, otherwise ref() returns a dead ref
            self.stopped = True
            ref._cell = None
            self._ref = None
        else:
            self.stopped = True
            ref = self.ref
        while True:
            try:
                sender, m = self.queue.get_nowait()
            except gevent.queue.Empty:
                break
            if m == ('_watched', ANY):
                self._watched(m[1])
            elif m == ('__error', ANY, ANY):
                _, exc, tb = m
                self.report((exc, tb))
            elif not (m == ('terminated', ANY) or m == ('_unwatched', ANY) or m == ('_node_down', ANY) or m == '_stop' or m == '_kill' or m == '__done' or m == '__undone'):
                Events.log(DeadLetter(ref, m, sender))
        self.parent_actor.send(('_child_terminated', ref))
        for watcher in (self.watchers or []):
            watcher << ('terminated', ref)
        self.actor = self.inbox = self.queue = self.parent_actor = None

    def unhandled(self, m, sender):
        if ('terminated', ANY) == m:
            raise UnhandledTermination(watcher=self.ref, watchee=m[1])
        else:
            Events.log(UnhandledMessage(self.ref, m, sender))

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
        actors = [self.spawn_actor(x, **kwargs) if isinstance(x, (type, Props)) else x for x in actors]
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
        if not self._ref or not self._ref():
            ref = Ref(cell=self, uri=self.uri, node=self.node)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()

    @property
    def root(self):
        from spinoff.actor.guardian import Guardian
        return self.parent_actor if isinstance(self.parent_actor, Guardian) else self.parent_actor._cell.root

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
