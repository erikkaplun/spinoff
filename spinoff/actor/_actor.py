# coding: utf-8
from __future__ import print_function

import abc
import inspect
import sys
import types
import traceback
import warnings
import weakref
from pickle import PicklingError
from collections import deque
from itertools import count, chain

from twisted.internet.defer import inlineCallbacks, Deferred
from txcoroutine import coroutine

from spinoff.util.pattern_matching import IS_INSTANCE, ANY
from spinoff.actor.events import UnhandledError, Events, UnhandledMessage, DeadLetter, ErrorIgnored, TopLevelActorTerminated
from spinoff.actor.supervision import Decision, Resume, Restart, Stop, Escalate, Default
from spinoff.actor.events import SupervisionFailure
from spinoff.util.async import call_when_idle_unless_already
from spinoff.util.async import with_timeout
from spinoff.util.async import Timeout
from spinoff.util.pattern_matching import Matcher
from spinoff.util.logging import Logging, logstring


# these messages get special handling from the framework and never reach Actor.receive
_SYSTEM_MESSAGES = ('_start', '_stop', '_restart', '_suspend', '_resume', ('_child_terminated', ANY))


def dbg(*args):
    print(file=sys.stderr, *args)


class NameConflict(Exception):
    pass


class Unhandled(Exception):
    pass


class UnhandledTermination(Exception):
    pass


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

    def __repr__(self):
        return 'CreateFailed(%r, %s, %s)' % (self.message, self.actor, repr(self.cause))


class BadSupervision(WrappingException):
    def __init__(self, message, exc, tb):
        WrappingException.__init__(self, message)
        self.cause, self.tb = exc, tb


# XXX: please unit-test this class thoroughly
class Uri(object):
    """Represents the identity and location of an actor.

    Attention: for the sake of consistency, the root `Uri` is represented by an empty string, **not** `'/'`. The `'/'` is
    used **only** as a path separator. Thus, both of the name and path path of the root `Uri` are `''`, and the steps
    are `['']`. The root `Uri` is also only `__eq__` to `''` and not `'/'`.

    """
    _node = None

    def __init__(self, name, parent, node=None):
        if name and node:
            raise TypeError("node specified for a non-root Uri")
        self.name, self.parent = name, parent
        if node:
            self._node = node

    @property
    def root(self):
        """Returns the topmost `Uri` this `Uri` is part of."""
        return self.parent.root if self.parent else self

    @property
    def node(self):
        """Returns the node ID this `Uri` points to."""
        return self.root._node

    def __div__(self, child):
        """Builds a new child `Uri` of this `Uri` with the given `name`."""
        if not child or not isinstance(child, str):
            raise TypeError("Uri traversal expected a non-empty str but got %r" % (child,))
        if child in ('.', '..'):
            raise TypeError("Traversing using . and .. is not supported (yet)")
        elif '/' in child:
            raise TypeError("Traversing more than 1 level at a time is not supported (yet)")
        return Uri(name=child, parent=self)

    @property
    def path(self):
        """Returns the `Uri` without the `node` part as a `str`."""
        return '/'.join(self.steps)

    @property
    def steps(self):
        """Returns an iterable containing the steps to this `Uri` from the root `Uri`, including the root `Uri`."""
        def _iter(uri, acc):
            acc.appendleft(uri.name if uri.name else '')
            return _iter(uri.parent, acc) if uri.parent else acc
        return _iter(self, acc=deque())

    def __str__(self):
        return (self.node or '') + self.path

    def __repr__(self):
        return '<@%s>' % (str(self),)

    @classmethod
    def parse(cls, addr):
        """Parses a new `Uri` instance from a string representation of a URI.

        >>> u1 = Uri.parse('/foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u2 = Uri.parse('somenode:123/foo/bar')
        >>> u2.node, u1.steps, u2.path, ur2.name
        ('somenode:123', ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u1 = Uri.parse('foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['foo', 'bar'], 'foo/bar', 'bar')

        """
        if addr.endswith('/'):
            raise ValueError("Uris must not end in '/'")
        parts = addr.split('/')
        if ':' in parts[0]:
            node, parts[0] = parts[0], ''
        else:
            # if parts[0] == '':
            #     parts[0] = None
            node = None

        ret = None  # Uri(name=None, parent=None, node=node) if node else None
        for step in parts:
            ret = Uri(name=step, parent=ret, node=node)
            node = None  # only set the node on the root Uri
        return ret

    @property
    def local(self):
        """Returns a copy of the `Uri` without the node. If the `Uri` has no node, returns the `Uri`."""
        if not self.node:
            return self
        else:
            return Uri.parse(self.path)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        """Returns `True` if `other` points to the same actor.

        This method is cooperative with the `pattern_matching` module.

        """
        if isinstance(other, str):
            other = Uri.parse(other)
        return str(self) == str(other) or isinstance(other, Matcher) and other == self


class RefBase(object):
    """Internal abstract class for all objects that behave like actor references."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def is_local(self):
        raise NotImplementedError

    @abc.abstractproperty
    def is_stopped(self):
        raise NotImplementedError

    def __lshift__(self, message):
        """A fancy looking alias to `RefBase.stop`, which in addition also supports chaining.

            someactor.send(msg1); someactor.send(msg2)
            someactor << msg1 << msg2

        """
        self.send(message)
        return self

    def stop(self):
        """Shortcut for `send('_stop')`"""
        self.send('_stop')


class _HubBound(object):
    """Internal abstract class for objects that depend on {being bound to/having available} a `Hub` instance."""
    _hub = None

    def __init__(self, hub):
        if hub:
            self._hub = hub

    @property
    def hub(self):
        """Returns the hub that this object should use for remote messaging and lookup.

        During `TESTING`, the `Node`, and from there on, all `_HubBound` objects, propagate the hub they are bound to
        down to any new `_HubBound` objects created. Otherwise, `Node.hub` is used.

        """
        assert self._hub if TESTING else not self._hub, (self, TESTING, self._hub)
        return self._hub or _NODE.hub


class Ref(RefBase, _HubBound):
    """A serializable, location-transparent, encapsulating reference to an actor.

    `Ref`s can be obtained in several different ways.

    Refs to already existing actors:

    * `Actor.node.guardian`: a ref to the `Guardian` the actor belongs to;

    * `Node.guardian`: a ref to the default `Guardian`
       (shouldn't be used fron inside an actor hierarchy if networkless, in-process testability is a goal, which it should be);

    * `self.ref`: a ref to the actor itself; this is meant to be the *only* channel an actor should ever be communicated
       through--an actor should *never* send out messages containing `self` and should instead *always* insert
       `self.ref` in messages sent out to other actors if it wants them to reach it back;

    By spawning new actors:

    * `self.spawn(...)`:

    * `self.node.spawn(...)`: a ref to a newly created subordinate actor of the spawning actor (a supervisor)

    * `spawn(...)`: **shouldn't be used directly,**; a ref to a newly spawned top-level actor in the default hierarchy;
       instead spawn a top-level actor by means of obtaining the `Node` whose hierarchy the spawning-actor is part of,
       or better yet, don't use top-level at all;
       (this is an alias for `Node.spawn`, in turn alias for `Node.guardian.spawn`)

    By looking up existing actors:

    * `Node.lookup(<uri or path>)`: looks up a local or remote actor;

    * `self.lookup(<path>)`: looks up a child actor

    Note:
    refs to `Guardian`s are not `Ref`s but merely objects that by all practical means have the same interface as Ref.

    """

    # XXX: should be protected/private
    cell = None  # so that .cell could be deleted to save memory
    is_local = True

    def __init__(self, cell, uri, is_local=True, hub=None):
        assert uri is None or isinstance(uri, Uri)
        super(Ref, self).__init__(hub=hub)
        if cell:
            assert isinstance(cell, Cell)
            self.cell = cell
        self.uri = uri
        if not is_local:
            assert not cell
            self.is_local = False

    def send(self, message, force_async=False):
        """Sends a message to the actor represented by this `Ref`.

        The send could but might not be asynchronous, depending on how the system has been configured and where the
        recipient is located.

        By default, sends to local actors are eager for better performance. This can be on a set per-call basis by
        passing `force_async=True` to this method, or overridden globally by setting `actor.SENDING_IS_ASYNC = True`;
        globally changing this is not recommended however unless you know what you're doing (e.g. during testing).

        """
        if self.cell:
            self.cell.receive(message, force_async=force_async)
        elif not self.is_local:
            self.hub.send_message(self, message)
        else:
            if ('_watched', ANY) == message:
                message[1].send(('terminated', self))
            elif message not in ('_stop', '_suspend', '_resume', '_restart', ('terminated', ANY)):
                Events.log(DeadLetter(self, message))

    def _stop_noevent(self):
        Events.consume_one(TopLevelActorTerminated)
        self.stop()

    @property
    def is_stopped(self):
        """Returns `True` if this actor is guaranteed to have stopped.

        If it returns `False`, it is not guaranteed that the actor isn't still running.

        """
        return self.is_local and not self.cell

    def join(self):
        future = Future()
        self << ('_watched', future)
        return future

    def __eq__(self, other):
        """Returns `True` if the `other` `Ref` points to the same actor.

        This method is cooperative with the `pattern_matching` module.

        """
        return (isinstance(other, Ref) and self.uri == other.uri
                or isinstance(other, Matcher) and other == self)

    def __repr__(self):  # TODO: distinguish local and remote
        return '<%s>' % (str(self.uri),)

    def __getstate__(self):
        # assert self.cell or not self.is_local, "TODO: if there is no cell and we're local, we should be returning a state that indicates a dead ref"
        # assert self.uri.node, "does not make sense to serialize a ref with no node: %r" % (self,)
        return str(self.uri)

    def __setstate__(self, uri):
        # if it's a tuple, it's a remote `Ref`, otherwise it must be just a local `Ref`
        # being pickled and unpickled for whatever reason:
        if isinstance(uri, tuple):
            self.is_local = False
            uri, self._hub = uri
        self.uri = Uri.parse(uri)


class _ActorContainer(object):
    _children = {}  # XXX: should be a read-only dict
    _child_name_gen = None

    def spawn(self, factory, name=None):
        """Spawns an actor using the given `factory` with the specified `name`.

        Returns an immediately usable `Ref` to the newly created actor, regardless of the location of the new actor, or
        when the actual spawning will take place.

        """
        if not self._children:
            self._children = {}
        uri = self.uri / name if name else None
        if name:
            if name.startswith('$'):
                raise ValueError("Unable to spawn actor at path %s; name cannot start with '$', it is reserved for auto-generated names" % (uri.path,))
            if name in self._children:
                raise NameConflict("Unable to spawn actor at path %s; actor %r already sits there" % (uri.path, self._children[name].cell.actor))
        if not uri:
            name = self._generate_name()
            uri = self.uri / name

        assert name not in self._children  # XXX: ordering??
        self._children[name] = None
        child = _do_spawn(parent=self.ref(), factory=factory, uri=uri)
        if name in self._children:  # it might have been removed already
            self._children[name] = child

        return child

    def _generate_name(self):
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return self._child_name_gen.next()

    def _reset(self):
        # XXX: not sure why but Python thinks these attributes don't exist sometimes
        try:
            del self._children
        except AttributeError:
            pass
        try:
            del self._child_name_gen
        except AttributeError:
            pass

    @property
    def children(self, child):
        return self._children.values()

    def _child_gone(self, child):
        name = child.uri.name  # rsplit('/', 1)[-1]
        del self._children[name]

    def lookup(self, uri):
        def rec(uri):
            if not uri.parent:
                return self
            else:
                child = rec(uri.parent)
                return child._children[uri.name]
        return rec(uri)


class Guardian(_ActorContainer, RefBase):
    """The root of an actor hierarchy.

    `Guardian` is a pseudo-actor in the sense that it's implemented in a way that makes it both an actor reference (but
    not a subclass of `Ref`) and an actor (but not a subclass of `Actor`). It only handles spawning of top-level
    actors, supervising them with the default guardian strategy, and taking care of stopping the entire system when
    told so.

    Obviously, unlike a normal actor, any other actor can directly spawn from under the/a `Guardian`.

    """
    is_local = True  # imitate Ref
    is_stopped = False  # imitate Ref

    def __init__(self, uri):
        self.uri = uri

    def ref(self):
        return self

    def send(self, message, force_async=False):
        if ('_error', ANY, IS_INSTANCE(Exception), IS_INSTANCE(types.TracebackType) | IS_INSTANCE(basestring)) == message:
            _, sender, exc, tb = message
            Events.log(UnhandledError(sender, exc, tb))
        elif ('_child_terminated', ANY) == message:
            _, sender = message
            self._child_gone(sender)
            Events.log(TopLevelActorTerminated(sender))
        elif '_stop' == message:
            self._do_stop()
        else:
            Events.log(UnhandledMessage(self, message))

    @inlineCallbacks
    def _do_stop(self):
        # dbg("GUARDIAN: stopping")
        for actor in self._children.values():
            # dbg("GUARDIAN: stopping", actor)
            actor.stop()
            # dbg("GUARDIAN: joining...", actor, actor.cell)
            try:
                yield with_timeout(.01, actor.join())
            except Timeout:
                # dbg("GUARDIAN: actor %r refused to stop" % (actor,))
                assert False, "actor %r refused to stop" % (actor,)
                # TODO: force-stop
            # dbg("GUARDIAN: ...OK", actor)

    def __getstate__(self):
        raise PicklingError("Guardian cannot be serialized")


class Node(object):
    """`Node` is both a singleton instance and a class lookalike, there is thus always available a default global
    `Node` instance but it is possible to create more, non-default, instances of `Node` by simply calling it as if it
    were a class, i.e. using it as a class. This is mainly useful for testing multi-node scenarios without any network
    involved by setting a custom `actor.remoting.Hub` to the `Node`.

    """
    hub = None

    def __init__(self, hub=None):
        guardian_uri = Uri(name=None, parent=None, node=hub.node if hub else None)
        self.guardian = Guardian(guardian_uri)

        self.set_hub(hub)

    def set_hub(self, hub):
        if hub:
            from .remoting import Hub
            if not isinstance(hub, Hub):
                raise TypeError("hub parameter to Guardian must be a %s.%s" % (Hub.__module__, Hub.__name__))
            if hub.guardian:
                raise RuntimeError("Can't bind Guardian to a Hub that is already bound to another Guardian")
            hub.guardian = self.guardian
            self.hub = hub

    def lookup(self, uri_or_addr):
        # TODO: move the local lookup logic to _ActorContainer.lookup
        if isinstance(uri_or_addr, Uri):
            if uri_or_addr.node == self.hub.node:
                return _ActorContainer.lookup(self, uri_or_addr)
            else:
                return Ref(cell=None, uri=uri_or_addr, is_local=False, hub=self.hub)
        else:
            if not uri_or_addr.startswith('/'):
                if not self.hub:
                    raise RuntimeError("Address lookups disabled when remoting is not enabled")
                return self.hub.lookup(uri_or_addr)
            else:
                return _ActorContainer.lookup(self, uri_or_addr)

    def spawn(self, *args, **kwargs):
        publish = kwargs.pop('publish', False)
        ret = self.guardian.spawn(*args, **kwargs)
        if publish:
            self.publish(ret)
        return ret

    def publish(self, actor):
        """Publishes the actor on the network.

        Make sure you've set a name to the actor and all its parents--otherwise a generated name will end up in the URI.

        """
        # it is meaningful to simply ignore register if there's no Hub available--this way a combination of actors in
        # general running distributedly can be run in a single node without any remoting needed at all without having
        # to change the code
        if self.hub:
            self.hub.register(actor)

    def stop(self):
        self.guardian.stop()

    def reset(self):
        self.guardian._reset()

    def __getstate__(self):
        raise PicklingError("Guardian cannot be serialized")

    def __repr__(self):
        return '<node:%s>' % (self._uri if self.hub else 'local')

    def __call__(self, hub=None):
        """Spawns new, non-default instances of the guardian; useful for testing."""
        return type(self)(hub)
Node = Node()

spawn = Node.spawn


class ActorType(abc.ABCMeta):  # ABCMeta to enable Process.run to be @abstractmethod
    def __new__(self, *args, **kwargs):
        """Automatically wraps any receive methods that are reported to be generators by `inspect` with
        `txcoroutine.coroutine`.

        """
        ret = super(ActorType, self).__new__(self, *args, **kwargs)
        if inspect.isgeneratorfunction(ret.receive):
            ret.receive = coroutine(ret.receive)
        if hasattr(ret, 'pre_start') and inspect.isgeneratorfunction(ret.pre_start):
            ret.pre_start = coroutine(ret.pre_start)
        if hasattr(ret, 'post_stop') and inspect.isgeneratorfunction(ret.post_stop):
            ret.post_stop = coroutine(ret.post_stop)
        return ret


class Actor(object):
    """Description here.

    __init__ should not attempt to access `self.ref` or `self.parent` as this are available only on an already
    initialized actor instance. If your initialization routine depends on either of those, use `pre_start` instead.

    """
    __metaclass__ = ActorType

    SPAWNING_IS_ASYNC = _DEFAULT_SPAWNING_IS_ASYNC = True
    SENDING_IS_ASYNC = _DEFAULT_SENDING_IS_ASYNC = False

    @classmethod
    def reset_flags(cls, debug=False):
        cls.SPAWNING_IS_ASYNC = False if debug else cls._DEFAULT_SPAWNING_IS_ASYNC
        cls.SENDING_IS_ASYNC = cls._DEFAULT_SENDING_IS_ASYNC

    __cell = None  # make it really private so it's hard and unpleasant to access the cell

    def receive(self, message):
        raise Unhandled

    def spawn(self, factory, name=None):
        return self.__cell.spawn(factory, name)

    @property
    def children(self):
        return self.__cell._children.values()

    def watch(self, other, self_ok=False):
        if other == self.ref:
            if not self_ok:
                warnings.warn("Portential problem: actor %s started watching itself; pass self_ok=True to mark as safe")
        else:
            other << ('_watched', self.ref)
        return other

    @property
    def ref(self):
        return self.__cell.ref()

    def _set_cell(self, cell):
        self.__cell = cell

    # TODO: this is a potential spot for optimisation: we could put the message directly in the Cell's inbox without
    # without invoking the receive logic which will not handle the message immediately anyway because we're processing
    # a message already--otherwise this method would not get called.
    def send(self, *args, **kwargs):
        """Alias for self.ref.send"""
        self.ref.send(*args, **kwargs)

    def __lshift__(self, message):
        self.ref.send(message)
        return self

    def stop(self):
        self.ref.stop()

    def __repr__(self):
        return "<actor-impl:%s@%s>" % (type(self).__name__, self.ref.uri)


class Props(object):
    def __init__(self, cls, *args, **kwargs):
        self.cls, self.args, self.kwargs = cls, args, kwargs

    def __call__(self):
        return self.cls(*self.args, **self.kwargs)

    def __repr__(self):
        args = ', '.join(repr(x) for x in self.args)
        kwargs = ', '.join('%s=%r' % x for x in self.kwargs.items())
        return '<props:%s(%s%s)>' % (self.cls.__name__, args, ((', ' + kwargs) if args else kwargs) if kwargs else '')


def _do_spawn(parent, factory, uri):
    cell = Cell(parent=parent, factory=factory, uri=uri)
    cell.receive('_start', force_async=Actor.SPAWNING_IS_ASYNC)
    return cell.ref()


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


class Cell(_ActorContainer, Logging):
    constructed = False
    started = False
    actor = None
    inbox = None
    priority_inbox = None

    # actor has begun shutting itself down but is waiting for all its children to stop first, and its own post_stop;
    # in the shutting-down state, an actor only accepts '_child_terminated' messages (and '_force_stop' in the future)
    shutting_down = False

    stopped = False

    suspended = False
    tainted = False  # true when init or pre_start failed and the actor is waiting for supervisor decision
    processing_messages = False
    _ongoing = None

    _ref = None

    _child_name_gen = None

    watchers = []

    def __init__(self, parent, factory, uri):
        if not callable(factory):
            raise TypeError("Provide a callable (such as a class, function or Props) as the factory of the new actor")
        self.parent = parent
        self.uri = uri

        self.factory = factory

        self.inbox = deque()
        self.priority_inbox = deque()

    @property
    def root(self):
        return self.parent if isinstance(self.parent, Guardian) else self.parent.cell.root

    # TODO: benchmark the message methods and optimise
    def has_message(self):
        return self.inbox or self.priority_inbox

    def consume_message(self):
        try:
            return self.priority_inbox.popleft()
        except IndexError:
            try:
                return self.inbox.popleft()
            except IndexError:
                return None

    def peek_message(self):
        try:
            return self.priority_inbox[0]
        except IndexError:
            try:
                return self.inbox[0]
            except IndexError:
                return None

    def logstate(self):
        return {'--\\': self.shutting_down, '+': self.stopped, 'N': not self.started,
                '_': self.suspended, '?': self.tainted, 'X': self.processing_messages, }

    def logcomment(self):
        if self.priority_inbox and self.inbox:
            def g():
                for i, msg in enumerate(chain(self.priority_inbox, self.inbox)):
                    yield msg if isinstance(msg, str) else repr(msg)
                    if i == 2:
                        yield '...'
                        return
            q = ', '.join(g())
            return '<<<[%s]' % (q,)
        else:
            return ''

    # TODO: change `force_async` to `async` and make it completely override the global setting, not just when it's `True`.
    @logstring(u'←')
    def receive(self, message, force_async=False):
        self.dbg(message if isinstance(message, str) else repr(message),)
        if self.stopped:
            return

        if self.shutting_down:
            # the shutting_down procedure is waiting for all children to terminate so we make an exception here
            # and handle the message directly, bypassing the standard message handling logic:
            # NB! DO NOT do this with a running actor--it changes the visible state of the actor
            if message == ('_child_terminated', ANY):
                _, child = message
                self._do_child_terminated(child)
            # don't care about any system message if we're already stopping:
            elif message not in _SYSTEM_MESSAGES:
                # so that it could be sent to dead letters when the stopping is complete:
                self.inbox.append(message)
            elif self.priority_inbox:
                self.priority_inbox.append(message)
            return

        # XXX: should ('terminated', child) also be prioritised?

        # '_watched' is something that is safe to handle immediately as it doesn't change the visible state of the actor;
        # NB: DO NOT do the same with '_suspend', '_resume' or any other message that changes the visible state of the actor!
        if message == ('_watched', ANY):
            self._do_watched(message[1])
        # ...except in case of an ongoing receive in which case the suspend-resume event will be seem atomic to the actor
        elif self._ongoing and message in ('_suspend', '_resume'):
            if message == '_suspend':
                self._do_suspend()
            else:
                self._do_resume()
        else:
            if message in _SYSTEM_MESSAGES:
                self.priority_inbox.append(message)
            else:
                self.inbox.append(message)
            self.process_messages(force_async=force_async)

    @logstring(u'↻')
    def process_messages(self, force_async=False):
        next_message = self.peek_message()
        self.dbg(next_message)

        is_startstop = next_message in ('_start', '_stop')
        is_untaint = next_message in ('_resume', '_restart')

        if not self.processing_messages and (self.started or is_startstop or self.tainted and is_untaint):
            if Actor.SENDING_IS_ASYNC or force_async:
                self.dbg(u'⇝')
                # dbg("PROCESS-MSGS: async (Actor.SENDING_IS_ASYNC? %s  force_async? %s" % (Actor.SENDING_IS_ASYNC, force_async))
                call_when_idle_unless_already(self._process_messages)  # TODO: check if there's an already scheduled call to avoid redundant calls
            else:
                self.dbg(u'↯')
                self._process_messages()
        else:
            self.dbg(u'  →X')

    @logstring(u'↻ ↻')
    @inlineCallbacks
    def _process_messages(self):
        self.dbg(self.peek_message())
        first = True
        try:
            while not self.stopped and (not self.shutting_down) and self.has_message() and (not self.suspended or self.peek_message() in ('_stop', '_restart', '_resume', '_suspend')) or (self.shutting_down and ('_child_terminated', ANY) == self.peek_message()):
                message = self.consume_message()
                self.processing_messages = repr(message)
                if not first:
                    self.dbg(u"↪ %r" % (message,))
                first = False
                try:
                    d = self._process_one_message(message)
                    # if isinstance(ret, Deferred) and not self.receive_is_coroutine:
                    #     warnings.warn(ConsistencyWarning("prefer yielding Deferreds from Actor.receive rather than returning them"))
                    yield d
                except Exception:
                    self.fail(u"☹")
                    self.report_to_parent()
                else:
                    self.dbg(message, "✓")
                finally:
                    self.processing_messages = False
            self.dbg("☺")
        except Exception:
            self.panic(u"!!BUG!!")
            self.report_to_parent()

    @logstring(u"↻ ↻ ↻")
    @inlineCallbacks
    def _process_one_message(self, message):
        self.dbg(message)
        if '_start' == message:
            yield self._do_start()
        elif ('_error', ANY, ANY, ANY) == message:
            _, sender, exc, tb = message
            yield self._do_supervise(sender, exc, tb)
        elif '_stop' == message:
            self._do_stop()
        elif '_restart' == message:
            yield self._do_restart()
        elif '_resume' == message:
            yield self._do_resume()
        elif '_suspend' == message:
            self._do_suspend()
        elif ('_child_terminated', ANY) == message:
            _, child = message
            self._do_child_terminated(child)
        else:
            receive = self.actor.receive
            try:
                self._ongoing = receive(message)
                # dbg("PROCESS-ONE: receive returned...", self._ongoing, self)
                # dbg("PROCESS-ONE: still processing?", self.processing_messages, self)
                yield self._ongoing
                # dbg("PROCESS-ONE: ...receive terminated", self)
                del self._ongoing
            except Unhandled:
                self._unhandled(message)
            except Exception:
                raise

    def _unhandled(self, message):
        if ('terminated', ANY) == message:
            raise UnhandledTermination
        else:
            Events.log(UnhandledMessage(self.ref(), message))

    @logstring("ctor:")
    @inlineCallbacks
    def _construct(self):
        self.dbg()
        factory = self.factory

        try:
            actor = factory()
        except Exception:
            self.fail(u"☹")
            raise CreateFailed("Constructing actor failed", factory)
        else:
            self.actor = actor

        actor._parent = self.parent
        actor._set_cell(self)

        if hasattr(actor, 'pre_start'):
            pre_start = actor.pre_start
            try:
                self._ongoing = pre_start()
                yield self._ongoing
                del self._ongoing
            except Exception:
                self.fail(u"☹")
                raise CreateFailed("Actor failed to start", actor)

        self.constructed = True
        self.dbg(u"✓")

    @logstring(u"►►")
    @inlineCallbacks
    def _do_start(self):
        # self.dbg()
        try:
            yield self._construct()
        except Exception:
            self.tainted = True
            raise
        else:
            self.started = True
            self.dbg(u"✓")

    @logstring("sup:")
    def _do_supervise(self, child, exc, tb):
        self.dbg1(u"%r ← %r" % (exc, child))
        if child not in self._children.values():  # TODO: use a denormalized set
            Events.log(ErrorIgnored(child, exc, tb))
            return

        supervise = getattr(self.actor, 'supervise', None)

        if supervise:
            decision = supervise(exc)
        if not supervise or decision == Default:
            # self.dbg("...fallback to default...")
            decision = default_supervise(exc)

        self.dbg3(u"→", decision)

        if not isinstance(decision, Decision):
            raise BadSupervision("Bad supervisor decision: %s" % (decision,), exc, tb)

        # TODO: make these always async?
        if Resume == decision:
            child.send('_resume')
        elif Restart(ANY, ANY) == decision:
            child.send('_restart')
        elif Stop == decision:
            child.send('_stop')
        else:
            raise exc, None, tb

    @logstring(u"||")
    def _do_suspend(self):
        self.dbg()
        self.suspended = True
        if self._ongoing:
            # self.dbg("ongoing.pause")
            self._ongoing.pause()
        if hasattr(self.actor, '_coroutine') and self.actor._coroutine:
            # self.dbg("calling coroutine.pause on", self.actor._coroutine)
            self.actor._coroutine.pause()

        for child in self._children.values():
            child.send('_suspend')

    @logstring(u"||►")
    def _do_resume(self):
        self.dbg()
        if self.tainted:
            self.dbg("...tainted → restarting...")
            warnings.warn("Attempted to resume an actor that failed to start; falling back to restarting:\n%s" % (''.join(traceback.format_stack()),))
            self.tainted = False
            return self._do_restart()
        else:
            # self.dbg("resuming...")
            self.suspended = False
            if self._ongoing:
                self._ongoing.unpause()
            if hasattr(self.actor, '_coroutine'):
                self.actor._coroutine.unpause()

            for child in self._children.values():
                child.send('_resume')
        self.dbg(u"✓")

    @logstring("stop:")
    def _do_stop(self):
        self.dbg()
        if self._ongoing:
            del self._ongoing
        # del self.watchers
        self._shutdown().addCallback(self._finish_stop)

    @logstring("finish-stop:")
    def _finish_stop(self, _):
        self.dbg()
        try:
            ref = self.ref()

            # TODO: test that system messages are not deadlettered
            for message in self.inbox:
                if ('_error', ANY, ANY, ANY) == message:
                    _, sender, exc, tb = message
                    Events.log(ErrorIgnored(sender, exc, tb))
                elif ('_watched', ANY) == message:
                    _, watcher = message
                    watcher.send(('terminated', ref))
                elif ('terminated', ANY) != message:
                    Events.log(DeadLetter(ref, message))

            if self.actor:
                self.actor = None
            del self.inbox
            del self.priority_inbox  # don't want no more, just release the memory

            # self.dbg("unlinking reference")
            del ref.cell
            self.stopped = True

            # XXX: which order should the following two operations be done?

            self.parent.send(('_child_terminated', ref))

            for watcher in self.watchers:
                watcher.send(('terminated', ref))
        except Exception:
            _, exc, tb = sys.exc_info()
            Events.log(ErrorIgnored(ref, exc, tb))
        self.dbg(u"✓")

    @logstring("watched:")
    def _do_watched(self, other):
        self.dbg(other)
        if self.stopped:
            other << ('terminated', self.ref())
            return
        if not self.watchers:
            self.watchers = []
        self.watchers.append(other)

    @logstring(u"► ↻")
    @inlineCallbacks
    def _do_restart(self):
        # self.dbg()
        self.suspended = True
        yield self._shutdown()
        yield self._construct()
        self.suspended = False
        self.dbg(u"✓")

    @logstring("child-term:")
    def _do_child_terminated(self, child):
        # TODO: PLEASE OPTIMISE
        # probably a child that we already stopped as part of a restart
        if child not in self._children.values():
            # LOGEVENT(TerminationIgnored(self, child))
            return
        self._child_gone(child)
        # itms = self._children.items()
        # ix = itms.index((ANY, child))
        # del self._children[itms[ix][0]]
        if self.shutting_down and not self._children:
            # dbg("CHILD-TERM: signalling all children stopped")
            self._all_children_stopped.callback(None)

    @logstring("shutdown:")
    @inlineCallbacks
    def _shutdown(self):
        # self.dbg()
        self.shutting_down = True

        if self._children:  # we don't want to do the Deferred magic if there're no babies
            self._all_children_stopped = Deferred()
            # dbg("SHUTDOWN: shutting down children:", self._children.values())
            for child in self._children.values():
                child.stop()
            # dbg("SHUTDOWN: waiting for all children to stop", self)
            yield self._all_children_stopped
            # dbg("SHUTDOWN: ...children stopped", self)

        if self.constructed and hasattr(self.actor, 'post_stop'):
            try:
                yield self.actor.post_stop()  # XXX: possibly add `yield` here
            except Exception:
                _ignore_error(self.actor)
        if hasattr(self.actor, '_coroutine'):
            try:
                self.actor._Process__shutdown()
            except Exception:
                _ignore_error(self.actor)

        self.actor = None
        self.shutting_down = False
        # self.dbg(u"✓")

    def report_to_parent(self, exc_and_tb=None):
        if not exc_and_tb:
            _, exc, tb = sys.exc_info()
        else:
            exc, tb = exc_and_tb
        try:
            # Events.log(Error(self, e, sys.exc_info()[2])),
            self._do_suspend()
            # XXX: might make sense to make it async by default for better latency
            self.parent.send(('_error', self.ref(), exc, tb), force_async=True)
        except Exception:
            try:
                Events.log(ErrorIgnored(self.ref(), exc, tb))
                _, sys_exc, sys_tb = sys.exc_info()
                Events.log(SupervisionFailure(self.ref(), sys_exc, sys_tb))
            except Exception:
                self.panic("failed to report:\n", traceback.format_exc(file=sys.stderr))

    def ref(self):
        if self.stopped:
            return Ref(cell=None, uri=self.uri)

        if not self._ref or not self._ref():
            ref = Ref(self, self.uri)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()

    def _generate_name(self):
        """Overrides ActorContext._generate_name"""
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return self._child_name_gen.next()

    def __repr__(self):
        return "<cell:%s@%s>" % (type(self.actor).__name__ if self.actor else (self.factory.__name__ if isinstance(self.factory, type) else repr(self.factory)),
                                 self.uri.path,)


# TODO: replace with serializable temporary actors
class Future(Deferred):  # TODO: ActorRefBase or IActorRef or smth
    def send(self, message):
        self.callback(message)


def _ignore_error(actor):
    _, exc, tb = sys.exc_info()
    Events.log(ErrorIgnored(actor, exc, tb))
