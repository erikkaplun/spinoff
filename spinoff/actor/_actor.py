# coding: utf-8
from __future__ import print_function

import abc
import inspect
import sys
import types
import traceback
import warnings
import weakref
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


class Path(str):
    @property
    def name(self):
        _, name = self.rsplit('/', 1)
        return name

    @property
    def parent(self):
        parent_path, _ = self.rsplit('/', 1)
        return parent_path

    @property
    def __div__(self, child):
        return self + '/' + child


# TODO: rename to Ref and use a Path str subtype for path
class Ref(object):
    # XXX: should be protected/private
    target = None  # so that .target could be deleted to save memory

    @classmethod
    def remote(cls, path, node, hub):
        proxy = hub.make_proxy(path, node)
        return Ref(proxy, path, node)

    def __init__(self, target, path, node=None):
        self.target = target
        self.path = Path(path)
        self.node = node

    def send(self, message, force_async=False):
        """Sends a message to this actor.

        The send could but might not be asynchronous, depending on how the system has been configured and where the
        recipient is located.

        By default, sends to local actors are eager for better performance. This can be on a set per-call basis by
        passing `force_async=True` to this method, or overridden globally by setting `actor.SENDING_IS_ASYNC = True`;
        globally changing this is not recommended however unless you know what you're doing (e.g. during testing).

        """
        if self.target:
            self.target.receive(message, force_async=force_async)
        elif ('_watched', ANY) == message:
            message[1].send(('terminated', self))
        elif message not in ('_stop', '_suspend', '_resume', '_restart', ('terminated', ANY)):
            Events.log(DeadLetter(self, message))

    def __lshift__(self, message):
        """See Ref.send"""
        self.send(message)
        return self

    def __repr__(self):
        return '<%s>' % (self.path,) if not self.node else '<%s%s>' % (self.node, self.path)

    def stop(self):
        """Shortcut for `Ref.send('_stop')`"""
        self.send('_stop')

    def _stop_noevent(self):
        Events.consume_one(TopLevelActorTerminated)
        self.stop()

    @property
    def is_stopped(self):
        """Returns `True` if this actor is known to have stopped.

        If it returns `False`, the actor still might be running.

        """
        return not self.target

    def join(self):
        future = Future()
        self << ('_watched', future)
        return future

    def __eq__(self, other):
        return (isinstance(other, Ref) and self.path == other.path and self.node == other.node
                or isinstance(other, Matcher) and other == self)

    def __getstate__(self):
        # we're probably being serialized for wire-transfer, so for the deserialized dopplegangers of us on other nodes
        # to be able to deliver to us messages, we need to register ourselves with the hub we belong to; if we're being
        # serialized for other reasons (such as storing to disk), well, tough luck--we'll have a redundant (weak)
        # reference to us in the `Hub`.
        assert self.target, "TODO: if there is no self.target, we should be returning a state that indicates a dead ref"

        if self.target._hub:
            self.target._hub.register(self)
        return {'path': self.path, 'node': self.node}


class _ActorContainer(object):
    _children = {}  # XXX: should be a read-only dict
    _child_name_gen = None

    _hub = None

    # TODO: add the `hub` parameter
    def spawn(self, factory, name=None, register=False):
        """Spawns an actor using the given `factory` with the specified `name`.

        If `register` is true, and if the actor is bound to a `Hub`, registers the actor with that `Hub`, otherwise.

        Returns an immediately `Ref` to the newly created actor, regardless of the location of the new actor, or
        when the actual spawning will take place.

        All child actors share the same `Hub`, which propagated down the hierarchy at spawn time (TODO: unless overridden).

        """
        if register:
            if not self._hub:
                raise TypeError("Can only auto-register actors spawned from a container bound to a Hub")

        if not self._children:
            self._children = {}
        path = self.path + ('' if self.path[-1] == '/' else '/') + name if name else None
        if name:
            if name.startswith('$'):
                raise ValueError("Unable to spawn actor at path %s; name cannot start with '$', it is reserved for auto-generated names" % (path,))
            if name in self._children:
                raise NameConflict("Unable to spawn actor at path %s; actor %r already sits there" % (path, self._children[name].target.actor))
        if not path:
            name = self._generate_name()
            path = '%s%s%s' % (self.path, ('' if self.path[-1] == '/' else '/'), name)
        assert name not in self._children  # XXX: ordering??
        self._children[name] = None
        child = _do_spawn(parent=self.ref(), factory=factory, path=path, hub=self._hub)
        if name in self._children:  # it might have been removed already
            self._children[name] = child

        # it is meaningful to simply ignore register if there's no Hub available--this way a combination of actors in
        # general running distributedly can be run in a single node without any remoting needed at all without having
        # to change the code
        if register and self._hub:
            self._hub.register(child)

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
        name = child.path.rsplit('/', 1)[-1]
        del self._children[name]


class Guardian(_ActorContainer):
    """The root of an actor hierarchy.

    `Guardian` is both a singleton instance and a class lookalike, there is thus always available a default global
    `Guardian` instance but it is possible to create more, non-default, instances of the `Guardian` by simply calling
    it as if it were a class, i.e. using it as a class. This is mainly useful for testing multi-node scenarios without
    any network involved by setting a custom `actor.remoting.Hub` to the `Guardian`.

    `Guardian` is a pseudo-actor in the sense that it's implemented in a way that makes it both an actor reference (but
    not a subclass of `Ref`) and an actor (but not a subclass of `Actor`). It only handles spawning of top-level
    actors, supervising them with the default guardian strategy, and taking care of stopping the entire system when
    told so.

    Obviously, unlike a normal actor, any other actor can directly spawn from under the/a `Guardian`.

    """
    path = '/'

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
        else:
            Events.log(UnhandledMessage(self, message))

    @inlineCallbacks
    def stop(self):
        # dbg("GUARDIAN: stopping")
        for actor in self._children.values():
            # dbg("GUARDIAN: stopping", actor)
            actor.stop()
            # dbg("GUARDIAN: joining...", actor, actor.target)
            try:
                yield with_timeout(.01, actor.join())
            except Timeout:
                # dbg("GUARDIAN: actor %r refused to stop" % (actor,))
                assert False, "actor %r refused to stop" % (actor,)
                # TODO: force-stop
            # dbg("GUARDIAN: ...OK", actor)

    def reset(self):
        self._reset()

    def __call__(self, hub=None):
        """Spawns new, non-default instances of the guardian; useful for testing."""
        ret = type(self)()
        ret._hub = hub
        return ret
Guardian = Guardian()


spawn = Guardian.spawn


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
        return "<actor-impl:%s@%s>" % (type(self).__name__, self.ref.path)


class Props(object):
    def __init__(self, cls, *args, **kwargs):
        self.cls, self.args, self.kwargs = cls, args, kwargs

    def __call__(self):
        return self.cls(*self.args, **self.kwargs)

    def __repr__(self):
        args = ', '.join(repr(x) for x in self.args)
        kwargs = ', '.join('%s=%r' % x for x in self.kwargs.items())
        return '<props:%s(%s%s)>' % (self.cls.__name__, args, ((', ' + kwargs) if args else kwargs) if kwargs else '')


def _do_spawn(parent, factory, path, hub=None):
    cell = Cell(parent=parent, factory=factory, path=path)
    cell._hub = hub
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
    _hub = None

    _child_name_gen = None

    watchers = []

    def __init__(self, parent, factory, path):
        if not callable(factory):
            raise TypeError("Provide a callable (such as a class, function or Props) as the factory of the new actor")
        self.parent = parent
        self.path = path

        self.factory = factory

        self.inbox = deque()
        self.priority_inbox = deque()

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
            del ref.target
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
            return Ref(None, path=self.path)

        if not self._ref or not self._ref():
            ref = Ref(self, self.path)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()

    def _generate_name(self):
        """Overrides ActorContext._generate_name"""
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return self._child_name_gen.next()

    def __repr__(self):
        return "<cell:%s@%s>" % (type(self.actor).__name__ if self.actor else (self.factory.__name__ if isinstance(self.factory, type) else repr(self.factory)),
                                 self.path,)


# TODO: replace with serializable temporary actors
class Future(Deferred):  # TODO: ActorRefBase or IActorRef or smth
    def send(self, message):
        self.callback(message)


def _ignore_error(actor):
    _, exc, tb = sys.exc_info()
    Events.log(ErrorIgnored(actor, exc, tb))
