from __future__ import print_function

import functools
import gc
import random
import re
import weakref
import time
import sys

from gevent import idle, sleep, GreenletExit, with_timeout, Timeout
from gevent.event import Event, AsyncResult
from gevent.queue import Channel, Empty, Queue
from nose.tools import eq_, ok_

from spinoff.actor import Actor, Props, Node, Uri
from spinoff.actor.ref import Ref
from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, ErrorIgnored, HighWaterMarkReached
from spinoff.actor.supervision import Ignore, Restart, Stop, Escalate, Default
from spinoff.remoting import Hub, HubWithNoRemoting
from spinoff.remoting.hublogic import HubLogic, Connect
from spinoff.actor.exceptions import InvalidEscalation, Unhandled, NameConflict, UnhandledTermination, CreateFailed, BadSupervision
from spinoff.util.pattern_matching import ANY, IS_INSTANCE
from spinoff.util.testing import (
    assert_raises, expect_one_warning, expect_one_event,
    expect_failure, MockActor, expect_event_not_emitted,)
from spinoff.actor.events import RemoteDeadLetter
from spinoff.util.testing.actor import wrap_globals
from spinoff.util.logging import dbg
from spinoff.util.python import deferred_cleanup


def wait(fn):
    while True:
        if fn():
            break
        else:
            idle()


class Observable(object):
    terminator = None

    def __init__(self):
        self.done = Channel()

    def wait_eq(self, terminator, message=None, timeout=None):
        if self.value == terminator:
            return self.value
        self.terminator = terminator
        try:
            with_timeout(timeout, self.done.get)
            return self.value
        except Timeout:
            eq_(self.value, terminator, message)

    def _check(self):
        if self.value == self.terminator:
            self.done.put(None)

    def __eq__(self, x):
        return self.value == x

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.value)


class obs_list(list, Observable):
    def __init__(self):
        list.__init__(self)
        Observable.__init__(self)

    value = property(lambda self: self)

    def do(x):
        def fn(self, *args, **kwargs):
            ret = getattr(list, x)(self, *args, **kwargs)
            self._check()
            return ret
        fn.__name__ = x
        return fn

    for x in ['append', 'extend', 'insert', 'pop', 'remove', 'reverse', 'sort']:
        locals()[x] = do(x)


class obs_count(Observable):
    value = 0

    def incr(self):
        self.value += 1
        self._check()


# ATTENTION: all tests functions are smartly auto-wrapped with wrap_globals at the bottom of this file.


##
## GC

def test_node_gc():
    node1 = Node(nid='localhost:20001', enable_remoting=False)
    ref = weakref.ref(node1)
    node1.stop()
    node1 = None
    gc.collect()
    ok_(not ref())

    node2 = Node(nid='localhost:20002', enable_remoting=True)
    ref = weakref.ref(node2)
    node2.stop()
    node2 = None
    gc.collect()
    ok_(not ref())


##
## SENDING & RECEIVING

@deferred_cleanup
def test_sent_message_is_received(defer):
    # Trivial send and receive.
    messages = obs_list()
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Props(MockActor, messages))
    a << 'foo'
    messages.wait_eq(['foo'])


@deferred_cleanup
def test_sending_operator_is_chainable(defer):
    # Trivial send and receive using the << operator.
    messages = obs_list()
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Props(MockActor, messages))
    a << 'foo' << 'bar'
    messages.wait_eq(['foo', 'bar'])


@deferred_cleanup
def test_receive_of_the_same_actor_never_executes_concurrently(defer):
    # Receive of the same actor is never concurrent.
    #
    # It is guaranteed that an actor that is processing
    # a message (i.e. is not idle), will not receive the next message before processing the previous one completes.
    #
    # This test case rules out only the basic recursive case with a non-switching receive method.
    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                receive_called.incr()
                self.ref << None
                eq_(receive_called.value, 1)
            else:
                receive_called.incr()

    node = DummyNode()
    defer(node.stop)
    receive_called = obs_count()
    node.spawn(MyActor) << 'init'
    receive_called.wait_eq(2)

    #

    class MyActor2(Actor):
        def receive(self, message):
            receive_called.incr()
            unblocked.wait()

    receive_called = obs_count()
    unblocked = Event()
    node = DummyNode()
    node.spawn(MyActor2) << None << None
    receive_called.wait_eq(1)
    unblocked.set()
    receive_called.wait_eq(2)


@deferred_cleanup
def test_unhandled_message_is_reported(defer):
    # Unhandled messages are reported to Events
    class MyActor(Actor):
        def receive(self, _):
            raise Unhandled
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    with expect_one_event(UnhandledMessage(a, 'foo')):
        a << 'foo'


@deferred_cleanup
def test_unhandled_message_to_guardian_is_also_reported(defer):
    node = DummyNode()
    defer(node.stop)
    guardian = node.guardian
    with expect_one_event(UnhandledMessage(guardian, 'foo')):
        guardian << 'foo'


@deferred_cleanup
def test_with_no_receive_method_all_messages_are_unhandled(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    with expect_one_event(UnhandledMessage(a, 'dummy')):
        a << 'dummy'


##
## SPAWNING

@deferred_cleanup
def test_spawning(defer):
    node = DummyNode()
    defer(node.stop)
    init_called, actor_spawned = Event(), Event()

    class MyActor(Actor):
        def __init__(self):
            init_called.set()

        def pre_start(self):
            actor_spawned.set()

    node.spawn(MyActor)
    ok_(not init_called.is_set())
    ok_(not actor_spawned.is_set())
    actor_spawned.wait()
    ok_(init_called.is_set())


@deferred_cleanup
def test_spawning_a_toplevel_actor_assigns_guardian_as_its_parent(defer):
    node = DummyNode()
    defer(node.stop)
    spawn = node.spawn
    pre_start_called = Event()

    class MyActor(Actor):
        def pre_start(self):
            ok_(self._parent is node.guardian)
            pre_start_called.set()

    spawn(MyActor)
    pre_start_called.wait()


@deferred_cleanup
def test_spawning_child_actor_assigns_the_spawner_as_parent(defer):
    node = DummyNode()
    defer(node.stop)
    childs_parent = AsyncResult()

    class MyActor(Actor):
        def receive(self, _):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            childs_parent.set(self._parent)

    a = node.spawn(MyActor)
    a << None
    ok_(childs_parent.get() is a)


@deferred_cleanup
def test_spawning_returns_an_immediately_usable_ref(defer):
    node = DummyNode()
    defer(node.stop)
    other_receive_called = Event()

    class MyActor(Actor):
        def receive(self, _):
            node.spawn(Other) << 'foo'

    class Other(Actor):
        def receive(self, _):
            other_receive_called.set()

    node.spawn(MyActor) << None
    other_receive_called.wait(), "Spawned actor should have received a message"


@deferred_cleanup
def test_pre_start_is_called_after_constructor_and_ref_and_parent_are_available(defer):
    node = DummyNode()
    defer(node.stop)
    message_received = Event()

    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                self.child = self.spawn(Child)
            else:
                ok_(message is self.child)
                message_received.set()

    class Child(Actor):
        def pre_start(self):
            self._parent << self.ref

    node.spawn(MyActor) << 'init'
    message_received.wait()


@deferred_cleanup
def test_errors_in_pre_start_are_reported_as_create_failed(defer):
    class MyActor(Actor):
        def pre_start(self):
            raise MockException()
    node = DummyNode()
    defer(node.stop)
    with expect_failure(CreateFailed) as basket:
        node.spawn(MyActor)
    with assert_raises(MockException):
        basket[0].raise_original()


@deferred_cleanup
def test_sending_to_self_does_not_deliver_the_message_until_after_the_actor_is_started(defer):
    class MyActor(Actor):
        def pre_start(self):
            self.ref << 'dummy'
            ok_(not message_received.is_set())

        def receive(self, message):
            message_received.set()

    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    node.spawn(MyActor)
    message_received.wait()


## REMOTE SPAWNING

# def test_TODO_remote_spawning():
#     pass


# def test_TODO_remotely_spawned_actors_ref_is_registered_eagerly():
#     # might not be necessary because a ref to the new remote child is returned anyway, and that does the registration;
#     # but then if the parent immediately sends another message before waiting for the spawning to return, the message
#     # will probably be dropped on the remote node
#     pass


# def test_TODO_remotely_spawned_actors_die_if_their_parent_node_seems_to_have_died():
#     # might not be necessary because a ref to the new remote child is returned anyway, and that does the registration;
#     # but then if the parent immediately sends another message before waiting for the spawning to return, the message
#     # will probably be dropped on the remote node
#     pass


# def test_TODO_remote_actorref_determinism():
#     pass


## REMOTE AUTO-DEPLOY

# def test_TODO_test_stub_registration_and_sending_of_eggs():
#     pass


##
## LIFECYCLE

@deferred_cleanup
def test_suspending(defer):
    node = DummyNode()
    defer(node.stop)

    message_received = Event()

    class MyActor(Actor):
        def receive(self, message):
            message_received.set()

    a = node.spawn(MyActor)
    a << 'foo'
    message_received.wait()
    message_received.clear()
    a << '_suspend'
    sleep(.001)
    ok_(not message_received.is_set())


def test_suspending_while_pre_start_is_blocked_pauses_pre_start():
    @deferred_cleanup
    def do(defer):
        class MyActor(Actor):
            def pre_start(self):
                released.wait()
                after_release.set()

        node = DummyNode()
        defer(node.stop)
        released = Event()
        after_release = Event()

        a = node.spawn(MyActor)
        sleep(.001)
        ok_(not after_release.is_set())
        a << '_suspend'
        released.set()
        sleep(.001)
        ok_(not after_release.is_set())

        a << '_resume'
        after_release.wait()
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


@deferred_cleanup
def test_suspending_with_nonempty_inbox_while_receive_is_blocked(defer):
    class MyActor(Actor):
        def receive(self, message):
            message_received.incr()
            if not released.is_set():  # only yield the first time for correctness
                released.wait()

    node = DummyNode()
    defer(node.stop)
    released = Event()
    message_received = obs_count()

    a = node.spawn(MyActor)
    a << None
    message_received.wait_eq(1)

    a << 'foo' << '_suspend'
    released.set()
    message_received.wait_eq(1)

    a << '_resume'
    message_received.wait_eq(2)


def test_suspending_while_receive_is_blocked_pauses_the_receive():
    @deferred_cleanup
    def do(defer):
        class MyActor(Actor):
            def receive(self, _):
                child_released.wait()
                after_release_reached.set()

        node = DummyNode()
        defer(node.stop)
        child_released, after_release_reached = Event(), Event()
        a = node.spawn(MyActor)
        a << 'dummy'
        sleep(.001)
        a << '_suspend'
        child_released.set()
        sleep(.001)
        ok_(not after_release_reached.is_set())

        a << '_resume'
        sleep(.001)
        ok_(after_release_reached.is_set())
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


@deferred_cleanup
def test_suspending_while_already_suspended(defer):
    # This can happen when an actor is suspended and then its parent gets suspended.
    class DoubleSuspendingActor(Actor):
        def receive(self, msg):
            message_received.set()
    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    node.spawn(DoubleSuspendingActor) << '_suspend' << '_suspend' << '_resume' << 'dummy'
    message_received.wait()


# def test_TODO_stopping():
#     pass


# def test_TODO_force_stopping_does_not_wait_for_a_deferred_post_stop_to_complete():
#     pass


@deferred_cleanup
def test_resuming(defer):
    class MyActor(Actor):
        def receive(self, message):
            message_received.incr()
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    a << '_suspend'
    message_received = obs_count()
    a << 'foo' << 'foo' << '_resume'
    message_received.wait_eq(2)


@deferred_cleanup
def test_restarting(defer):
    class MyActor(Actor):
        def pre_start(self):
            actor_started.incr()

        def receive(self, _):
            message_received.set()

        def post_stop(self):
            post_stop_called.set()

    actor_started = obs_count()
    message_received, post_stop_called = Event(), Event()

    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    actor_started.wait_eq(1)
    a << '_restart'
    idle()
    ok_(not message_received.is_set())
    actor_started.wait_eq(2)
    post_stop_called.wait()


@deferred_cleanup
def test_restarting_stopped_actor_has_no_effect(defer):
    class MyActor(Actor):
        def pre_start(self):
            actor_started.incr()

    node = DummyNode()
    defer(node.stop)
    actor_started = obs_count()
    a = node.spawn(MyActor)
    a.stop()
    a << '_restart'
    sleep(.001)
    eq_(actor_started, 1)


@deferred_cleanup
def test_restarting_doesnt_destroy_the_inbox(defer):
    class MyActor(Actor):
        def pre_start(self):
            started.incr()

        def receive(self, message):
            messages_received.append(message)

    node = DummyNode()
    defer(node.stop)
    messages_received = obs_list()
    started = obs_count()
    a = node.spawn(MyActor)
    a << 'foo' << '_restart' << 'bar'
    messages_received.wait_eq(['foo', 'bar'])


@deferred_cleanup
def test_restarting_waits_till_the_ongoing_receive_is_complete(defer):
    # Restarts are not carried out until the current receive finishes.
    class MyActor(Actor):
        def pre_start(self):
            started.incr()

        def receive(self, message):
            receive_started.set()
            if started == 1:
                released.wait()

    node = DummyNode()
    defer(node.stop)
    started = obs_count()
    receive_started, released = Event(), Event()
    a = node.spawn(MyActor)
    a << 'foo'
    receive_started.wait()
    a << '_restart'
    sleep(.001)
    eq_(started, 1)
    released.set()
    started.wait_eq(2)


@deferred_cleanup
def test_restarting_does_not_complete_until_pre_start_completes(defer):
    class MyActor(Actor):
        def pre_start(self):
            if not started:
                started.set()
            else:
                released.wait()  # only block on restart, not on creation

        def receive(self, message):
            received.set()

    node = DummyNode()
    defer(node.stop)
    released, received, started = Event(), Event(), Event()

    a = node.spawn(MyActor)
    a << '_restart' << 'dummy'
    sleep(.001)
    ok_(not received.is_set())
    released.set()
    received.wait()


@deferred_cleanup
def test_actor_is_untainted_after_a_restart(defer):
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            return (Restart if isinstance(exc, CreateFailed) and isinstance(exc.cause, MockException) else
                    Ignore if isinstance(exc, MockException) else
                    Escalate)

    class Child(Actor):
        def pre_start(self):
            if not started_once.is_set():
                started_once.set()
                raise MockException

        def receive(self, _):
            if not received_once.is_set():
                received_once.set()
                raise MockException
            else:
                message_received.set()

    node = DummyNode()
    defer(node.stop)
    started_once, received_once, message_received = Event(), Event(), Event()
    child = AsyncResult()
    node.spawn(Parent)
    child.get() << 'dummy1'
    received_once.wait()
    child.get() << 'dummy'
    message_received.wait()


@deferred_cleanup
def test_stopping_waits_till_the_ongoing_receive_is_complete(defer):
    class MyActor(Actor):
        def receive(self, message):
            released.wait()

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped, released = Event(), Event()
    a = node.spawn(MyActor) << 'foo'
    sleep(.001)
    a.stop()
    ok_(not stopped.is_set())
    released.set()
    stopped.wait()


@deferred_cleanup
def test_messages_sent_by_child_post_stop_to_restarting_parent_are_processed_after_restart(defer):
    class Parent(Actor):
        def pre_start(self):
            parent_started.incr()
            if parent_started == 1:
                self.spawn(Child)

        def receive(self, message):
            eq_(parent_started, 2)
            parent_received.set()

    class Child(Actor):
        def post_stop(self):
            self._parent << 'should-be-received-after-restart'

    node = DummyNode()
    defer(node.stop)
    parent_started = obs_count()
    parent_received = Event()
    node.spawn(Parent) << '_restart'
    parent_received.wait()


@deferred_cleanup
def test_stopping_an_actor_prevents_it_from_processing_any_more_messages(defer):
    class MyActor(Actor):
        def receive(self, _):
            received.set()
    node = DummyNode()
    defer(node.stop)
    received = Event()
    a = node.spawn(MyActor)
    a << None
    received.wait()
    received.clear()
    a.stop()
    sleep(.001)
    ok_(not received.is_set(), "the '_stop' message should not be receivable in the actor")
    with expect_one_event(DeadLetter(a, None)):
        a << None


@deferred_cleanup
def test_stopping_calls_post_stop(defer):
    class MyActor(Actor):
        def post_stop(self):
            post_stop_called.set()
    node = DummyNode()
    defer(node.stop)
    post_stop_called = Event()
    node.spawn(MyActor).stop()
    post_stop_called.wait()


@deferred_cleanup
def test_stopping_waits_for_post_stop(defer):
    class ChildWithDeferredPostStop(Actor):
        def post_stop(self):
            stop_complete.wait()

    class Parent(Actor):
        def pre_start(self):
            self.child = self.watch(self.spawn(ChildWithDeferredPostStop))

        def receive(self, message):
            if message == 'stop-child':
                self.child.stop()
            else:
                parent_received.set()

    node = DummyNode()
    defer(node.stop)
    stop_complete, parent_received = Event(), Event()
    node.spawn(Parent) << 'stop-child'
    sleep(.001)
    ok_(not parent_received.is_set())
    stop_complete.set()
    parent_received.wait()


@deferred_cleanup
def test_parent_is_stopped_before_children(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def post_stop(self):
            parent_stopping.set()
            unblocked.wait()

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    node = DummyNode()
    defer(node.stop)
    parent_stopping, unblocked, child_stopped = Event(), Event(), Event()
    a = node.spawn(Parent)
    a.stop()
    parent_stopping.wait()
    ok_(not child_stopped.is_set())
    unblocked.set()
    child_stopped.wait()


@deferred_cleanup
def test_actor_is_not_restarted_until_its_children_are_stopped(defer):
    class Parent(Actor):
        def pre_start(self):
            parent_started.incr()
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            stop_complete.wait()

    node = DummyNode()
    defer(node.stop)
    stop_complete = Event()
    parent_started = obs_count()
    node.spawn(Parent) << '_restart'
    parent_started.wait_eq(1)
    stop_complete.set()
    parent_started.wait_eq(2)


@deferred_cleanup
def test_error_reports_to_a_stopping_actor_are_ignored(defer):
    class Child(Actor):
        def receive(self, _):
            child_released.wait()
            raise MockException()

    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

    node = DummyNode()
    defer(node.stop)
    child = AsyncResult()
    child_released = Event()
    p = node.spawn(Parent)
    child.get() << 'dummy'
    p.stop()
    with expect_one_event(ErrorIgnored(child.get(), ANY, ANY)):
        child_released.set()


@deferred_cleanup
def test_stopping_in_pre_start_directs_any_refs_to_deadletters(defer):
    class MyActor(Actor):
        def pre_start(self):
            self.stop()

        def receive(self, message):
            message_received.set()

    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    a = node.spawn(MyActor)
    with expect_one_event(DeadLetter(a, 'dummy')):
        a << 'dummy'
    ok_(not message_received.is_set())


@deferred_cleanup
def test_stop_message_received_twice_is_silently_ignored(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    a.stop()
    sleep(.001)


# def test_TODO_kill():
#     pass


# def test_TODO_poisonpill():
#     pass


# ## REMOTE LIFECYCLE


# def test_TODO_remote_suspending():
#     pass


# def test_TODO_remote_resuming():
#     pass


# def test_TODO_remote_restarting():
#     pass


# def test_TODO_remote_stopping():
#     pass


##
## ACTORREFS, URIS & LOOKUP

# def test_TODO_actorrefs_with_equal_paths_are_equal():
#     assert Ref(None, path='123') == Ref(None, path='123')


@deferred_cleanup
def test_actors_are_garbage_collected_on_termination(defer):
    class MyActor(Actor):
        def __del__(self):
            del_called.set()

    node = DummyNode()
    defer(node.stop)
    del_called = Event()
    node.spawn(MyActor).stop()
    idle()
    gc.collect()
    ok_(del_called.is_set())


@deferred_cleanup
def test_cells_are_garbage_collected_on_termination(defer):
    node = DummyNode()
    defer(node.stop)
    ac = node.spawn(Actor)
    cell = weakref.ref(ac._cell)
    ok_(cell())
    ac.stop()
    idle()
    gc.collect()
    ok_(not cell())


@deferred_cleanup
def test_messages_to_dead_actors_are_sent_to_dead_letters(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    with expect_one_event(DeadLetter(a, 'should-end-up-as-letter')):
        a << 'should-end-up-as-letter'
        idle()


@deferred_cleanup
def test_guardians_path_is_the_root_uri(defer):
    node = DummyNode()
    defer(node.stop)
    eq_(node.guardian.uri.path, '')


@deferred_cleanup
def test_toplevel_actorrefs_paths_are_prefixed_with_guardians_path(defer):
    node = DummyNode()
    defer(node.stop)
    eq_(node.spawn(Actor, name='a').uri.path, '/a')
    eq_(node.spawn(Actor, name='b').uri.path, '/b')


@deferred_cleanup
def test_non_toplevel_actorrefs_are_prefixed_with_their_parents_path(defer):
    class MyActor(Actor):
        def pre_start(self):
            child_ref.set(self.spawn(Actor, name='child'))
    node = DummyNode()
    defer(node.stop)
    child_ref = AsyncResult()
    a = node.spawn(MyActor, name='parent')
    eq_(child_ref.get().uri.path, a.uri.path + '/child')


@deferred_cleanup
def test_toplevel_actor_paths_must_be_unique(defer):
    node = DummyNode()
    defer(node.stop)
    node.spawn(Actor, name='a')
    with assert_raises(NameConflict):
        node.spawn(Actor, name='a')


def test_non_toplevel_actor_paths_must_be_unique():
    class MyActor(Actor):
        def pre_start(self):
            self.spawn(Actor, name='a')
            with assert_raises(NameConflict):
                self.spawn(Actor, name='a')
            spawned.set()
    node = DummyNode()
    spawned = Event()
    node.spawn(MyActor, name='a')
    spawned.wait()


@deferred_cleanup
def test_spawning_toplevel_actors_without_name_assigns_autogenerated_names(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    ok_(re.match(r'/[^/]+$', a.uri.path))
    b = node.spawn(Actor)
    ok_(re.match(r'/[^/]+$', b.uri.path))
    ok_(b.uri.path != a.uri.path)


@deferred_cleanup
def test_spawning_non_toplevel_actors_without_name_assigns_autogenerated_names_with_prefixed_parent_path(defer):
    class MyActor(Actor):
        def pre_start(self):
            l = len(self.ref.uri.path)
            a = self.spawn(Actor)
            ok_(a.uri.path[l:])
            ok_(a.uri.path.startswith(self.ref.uri.path + '/'), a.uri.path)
            b = self.spawn(Actor)
            ok_(b.uri.path[l:])
            ok_(a.uri.path != b.uri.path)
            started.set()
    node = DummyNode()
    defer(node.stop)
    started = Event()
    node.spawn(MyActor)
    started.wait()


@deferred_cleanup
def test_spawning_with_autogenerated_looking_name_raises_an_exception(defer):
    node = DummyNode()
    defer(node.stop)
    with assert_raises(ValueError):
        node.spawn(Actor, name='$1')


## URIs

def test_relative_uri():
    #
    uri = Uri.parse('foo')
    eq_(str(uri), 'foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, 'foo')
    ok_(not uri.url)

    eq_(uri, Uri.parse('foo'))
    eq_(uri, 'foo', "Uri.__eq__ supports str")

    ok_(not uri.parent)
    ok_(not uri.node)
    eq_(uri.root, uri)

    eq_(list(uri.steps), ['foo'])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('foo/bar')
    eq_(str(uri), 'foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri.path, 'foo/bar')
    ok_(not uri.url)

    eq_(uri, Uri.parse('foo/bar'))
    eq_(uri, 'foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, 'foo')
    ok_(not uri.node)
    eq_(uri.root, 'foo')

    eq_(list(uri.steps), ['foo', 'bar'])

    ok_(uri.local is uri)


def test_absolute_uri():
    #
    uri = Uri.parse('')
    eq_(str(uri), '')
    eq_(uri.name, '')
    eq_(uri.path, '')
    ok_(not uri.url)

    eq_(uri, Uri.parse(''))
    eq_(uri, '')

    ok_(not uri.parent)
    ok_(not uri.node)
    eq_(uri.root, uri)

    eq_(list(uri.steps), [''])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('/foo')
    eq_(str(uri), '/foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, '/foo')
    ok_(not uri.url)

    eq_(uri, Uri.parse('/foo'))
    eq_(uri, '/foo', "Uri.__eq__ supports str")

    ok_(uri.parent == '')
    ok_(not uri.node)
    eq_(uri.root, '')

    eq_(list(uri.steps), ['', 'foo'])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('/foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri, '/foo/bar', "Uri.__eq__ supports str")
    eq_(uri.path, '/foo/bar')
    ok_(not uri.url)

    eq_(uri, Uri.parse('/foo/bar'))
    eq_(uri, '/foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, '/foo')
    ok_(not uri.node)
    eq_(uri.root, '')

    eq_(list(uri.steps), ['', 'foo', 'bar'])

    ok_(uri.local is uri)


def test_fully_qualified_uri():
    #
    uri = Uri.parse('localhost:123')
    eq_(str(uri), 'localhost:123')
    eq_(uri.name, '')
    eq_(uri.path, '')
    eq_(uri.url, 'tcp://localhost:123')

    eq_(uri, Uri.parse('localhost:123'))
    eq_(uri, 'localhost:123')

    ok_(not uri.parent)
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, uri)

    eq_(list(uri.steps), [''])

    eq_(uri.local, '')

    #
    uri = Uri.parse('localhost:123/foo')
    eq_(str(uri), 'localhost:123/foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, '/foo')
    eq_(uri.url, 'tcp://localhost:123/foo')

    eq_(uri, Uri.parse('localhost:123/foo'))
    eq_(uri, 'localhost:123/foo', "Uri.__eq__ supports str")

    eq_(uri.parent, 'localhost:123')
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, 'localhost:123')

    eq_(list(uri.steps), ['', 'foo'])

    eq_(uri.local, '/foo')

    #
    uri = Uri.parse('localhost:123/foo/bar')
    eq_(str(uri), 'localhost:123/foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri.path, '/foo/bar')
    eq_(uri.url, 'tcp://localhost:123/foo/bar')

    eq_(uri, Uri.parse('localhost:123/foo/bar'))
    eq_(uri, 'localhost:123/foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, 'localhost:123/foo')
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, 'localhost:123')

    eq_(list(uri.steps), ['', 'foo', 'bar'])

    eq_(uri.local, '/foo/bar')


def test_uri_hash():
    eq_(hash(Uri.parse('foo')), hash(Uri.parse('foo')))
    eq_(hash(Uri.parse('/foo')), hash(Uri.parse('/foo')))
    eq_(hash(Uri.parse('localhost:123/foo')), hash(Uri.parse('localhost:123/foo')))


## LOOKUP

@deferred_cleanup
def test_looking_up_an_actor_by_its_absolute_path_returns_the_original_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup_str('/toplevel') is toplevel_actor)
    ok_(node.lookup(Uri.parse('/toplevel')) is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    ok_(node.lookup_str('/toplevel/child') is child_actor)
    ok_(node.lookup(Uri.parse('/toplevel/child')) is child_actor)


@deferred_cleanup
def test_looking_up_an_actor_by_a_relative_path_returns_the_original_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup_str('toplevel') is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    ok_(node.lookup_str('toplevel/child') is child_actor)
    ok_(toplevel_actor / 'child' is child_actor)


@deferred_cleanup
def test_looking_up_an_actor_by_a_parent_traversing_relative_path_returns_a_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)

    a = node.spawn(Actor, name='a')
    ok_(node.guardian / 'a' is a)

    b = a._cell.spawn(Actor, name='b')
    ok_(a / 'b' is b)
    ok_(node.guardian / 'a/b' is b)


@deferred_cleanup
def test_looking_up_an_absolute_path_as_if_it_were_relative_just_does_an_absolute_lookup(defer):
    node = DummyNode()
    defer(node.stop)

    a = node.spawn(Actor, name='a')
    a._cell.spawn(Actor, name='b')
    root_b = node.spawn(Actor, name='b')

    eq_(a / '/b', root_b)


@deferred_cleanup
def test_looking_up_a_non_existent_local_actor_raises_runtime_error(defer):
    node = DummyNode()
    defer(node.stop)
    with assert_raises(RuntimeError):
        node.guardian / 'noexist'


# def test_looking_up_a_non_existent_local_actor_returns_a_dead_ref_with_nevertheless_correct_uri():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')
#     eq_(node.guardian, node.lookup('local:123'))

#     noexist = node.lookup('local:123/a/b/c')
#     eq_(noexist.uri, 'local:123/a/b/c')
#     ok_(noexist.is_local)
#     with expect_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'

#     hypotheticalchild = noexist / 'hypotheticalchild'
#     ok_(hypotheticalchild.is_local)
#     eq_(hypotheticalchild.uri, 'local:123/a/b/c/hypotheticalchild')
#     with expect_one_event(DeadLetter(hypotheticalchild, 'foo')):
#         hypotheticalchild << 'foo'


# def test_manually_ceated_remote_looking_ref_to_a_non_existend_local_actor_is_converted_to_dead_ref_on_send():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')

#     noexist = Ref(cell=None, is_local=False, uri=Uri.parse('local:123/a/b/c'), hub=node.hub)

#     with expect_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'
#     ok_(noexist.is_local)


##
## SUPERVISION & ERROR HANDLING

@deferred_cleanup
def test_supervision_decision_resume(defer):
    # Child is resumed if `supervise` returns `Ignore`
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise' << 'other-message'

        def supervise(self, _):
            return Ignore

    class Child(Actor):
        def receive(self, message):
            if message == 'raise':
                raise MockException
            else:
                message_received.set()

    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    node.spawn(Parent)
    message_received.wait()


@deferred_cleanup
def test_supervision_decision_restart(defer):
    # Child is restarted if `supervise` returns `Restart`
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Restart

    class Child(Actor):
        def pre_start(self):
            child_started.incr()

        def receive(self, message):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    child_started = obs_count()
    node.spawn(Parent)
    child_started.wait_eq(2)


@deferred_cleanup
def test_supervision_decision_stop(defer):
    # Child is stopped if `supervise` returns `Stop`
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Stop

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

        def receive(self, message):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    child_stopped = Event()
    node.spawn(Parent)
    child_stopped.wait()


@deferred_cleanup
def test_supervision_decision_escalate(defer):
    class ParentsParent(Actor):
        def pre_start(self):
            self.spawn(Parent)

        def supervise(self, exc):
            escalated.set()
            return Stop

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Escalate

    class Child(Actor):
        def receive(self, message):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    escalated = Event()
    node.spawn(ParentsParent)
    escalated.wait()


# def test_TODO_exception_escalations_are_reported_as_events():
#     pass


@deferred_cleanup
def test_error_suspends_actor(defer):
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            ok_(isinstance(exc, MockException))
            parent_received_error.set()
            parent_supervise_released.wait()
            return Ignore

    class Child(Actor):
        def receive(self, message):
            if message == 'cause-error':
                raise MockException
            else:
                child_received_message.set()

    node = DummyNode()
    defer(node.stop)
    child = AsyncResult()
    parent_received_error, parent_supervise_released, child_received_message = Event(), Event(), Event()

    node.spawn(Parent)
    child.get() << 'cause-error' << 'dummy'
    parent_received_error.wait()
    ok_(not child_received_message.is_set(), "actor should not receive messages after error until being resumed")
    parent_supervise_released.set()
    child_received_message.wait()


@deferred_cleanup
def test_error_suspends_children(defer):
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            parent_supervise_released.wait()
            return Stop

    class Child(Actor):
        def pre_start(self):
            childchild.set(self.spawn(ChildChild))

        def receive(self, message):
            raise MockException

    class ChildChild(Actor):
        def receive(self, message):
            received.set()

    parent_supervise_released = Event()
    child, childchild = AsyncResult(), AsyncResult()
    received = Event()

    node = DummyNode()
    defer(node.stop)

    node.spawn(Parent)
    child = child.get()
    child << 'cause-error'
    childchild = childchild.get()
    sleep(.05)
    childchild << 'foo'
    try:
        ok_(not received.is_set())
    finally:
        parent_supervise_released.set()


@deferred_cleanup
def test_exception_after_stop_is_ignored_and_does_not_report_to_parent(defer):
    class Child(Actor):
        def receive(self, _):
            self.stop()
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'dummy'

        def supervise(self, exc):
            ok_(False, "should not reach here")

    node = DummyNode()
    defer(node.stop)
    with expect_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        node.spawn(Parent)


@deferred_cleanup
def test_error_in_post_stop_prints_but_doesnt_report_to_parent_and_termination_messages_are_sent_as_normal(defer):
    class Child(Actor):
        def post_stop(self):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.child = self.spawn(Child)
            self.watch(self.child)
            self.child.stop()

        def supervise(self, exc):
            ok_(False, "should not reach here")

        def receive(self, message):
            eq_(message, ('terminated', self.child))
            termination_message_received.set()

    node = DummyNode()
    defer(node.stop)
    termination_message_received = Event()
    with expect_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        node.spawn(Parent)
        idle()
    termination_message_received.wait()


@deferred_cleanup
def test_supervision_message_is_handled_directly_by_supervise_method(defer):
    class Parent(Actor):
        def supervise(self, exc):
            supervise_called.set()
            exc_received.set(exc)
            return Stop

        def pre_start(self):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            raise exc

    node = DummyNode()
    defer(node.stop)
    supervise_called = Event()
    exc_received = AsyncResult()
    exc = MockException('arg1', 'arg2')

    node.spawn(Parent)
    supervise_called.wait()
    ok_(isinstance(exc_received.get(), MockException))
    ok_(exc_received.get().args, exc.args)


@deferred_cleanup
def test_init_error_reports_to_supervisor(defer):
    class ChildWithFailingInit(Actor):
        def __init__(self):
            raise MockException

    class Parent(Actor):
        def supervise(self, exc):
            received_exception.set(exc)
            return Stop

        def pre_start(self):
            self.spawn(ChildWithFailingInit)

    node = DummyNode()
    defer(node.stop)
    received_exception = AsyncResult()
    node.spawn(Parent)
    exc = received_exception.get()
    ok_(isinstance(exc, CreateFailed))
    with assert_raises(MockException):
        exc.raise_original()


@deferred_cleanup
def test_pre_start_error_reports_to_supervisor(defer):
    class ChildWithFailingInit(Actor):
        def pre_start(self):
            raise MockException

        def post_stop(self):
            post_stop_called()

    class Parent(Actor):
        def supervise(self, exc):
            received_exception.set(exc)
            return Stop

        def pre_start(self):
            self.spawn(ChildWithFailingInit)

    node = DummyNode()
    defer(node.stop)
    received_exception = AsyncResult()
    post_stop_called = Event()
    node.spawn(Parent)
    exc = received_exception.get()
    ok_(isinstance(exc, CreateFailed))
    with assert_raises(MockException):
        exc.raise_original()
    ok_(not post_stop_called.is_set(), "post_stop must not be called if there was an error in pre_start")


@deferred_cleanup
def test_receive_error_reports_to_supervisor(defer):
    class Parent(Actor):
        def supervise(self, exc):
            received_exception.set(exc)
            return Stop

        def pre_start(self):
            self.spawn(ChildWithExcInReceive) << None

    class ChildWithExcInReceive(Actor):
        method_under_test = 'receive'

        def receive(self, _):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    received_exception = AsyncResult()
    node.spawn(Parent)
    ok_(isinstance(received_exception.get(), MockException),
        "Child errors in the 'receive' method should be sent to its parent")


@deferred_cleanup
def test_restarting_or_resuming_an_actor_that_failed_to_init_or_in_pre_start(defer):
    class Parent(Actor):
        def __init__(self, child_cls, supervisor_decision):
            self.child_cls = child_cls
            self.supervisor_decision = supervisor_decision

        def supervise(self, exc):
            return self.supervisor_decision

        def pre_start(self):
            self.spawn(self.child_cls)

    ##

    class ChildWithErrorInInit(Actor):
        def __init__(self):
            child_created.incr()
            if not init_already_raised.is_set():  # avoid infinite ping-pong
                init_already_raised.set()
                raise MockException

    node = DummyNode()
    defer(node.stop)
    init_already_raised = Event()
    child_created = obs_count()
    node.spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Restart))
    child_created.wait_eq(2)
    #
    init_already_raised = Event()
    child_created = obs_count()
    node.spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Ignore))
    child_created.wait_eq(1, message="resuming a tainted actor stops it")

    ##

    class ChildWithErrorInPreStart(Actor):
        def pre_start(self):
            child_started.incr()
            if not pre_start_already_raised.is_set():  # avoid infinite ping-pong
                pre_start_already_raised.set()
                raise MockException

    node = DummyNode()
    defer(node.stop)
    pre_start_already_raised = Event()
    child_started = obs_count()
    node.spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Restart))
    child_started.wait_eq(2)
    #
    pre_start_already_raised = Event()
    child_started = obs_count()
    node.spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Ignore))
    child_started.wait_eq(1, message="resuming a tainted actor stops it")


@deferred_cleanup
def test_error_report_after_restart_is_ignored(defer):
    class Parent(Actor):
        def pre_start(self):
            if not child.ready():  # so that after the restart the child won't exist
                child.set(self.spawn(Child))

    class Child(Actor):
        def receive(self, _):
            child_released.wait()
            raise MockException

    node = DummyNode()
    defer(node.stop)
    child = AsyncResult()
    child_released = Event()

    parent = node.spawn(Parent)
    child.get() << 'dummy'
    parent << '_restart'
    with expect_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        child_released.set()


@deferred_cleanup
def test_default_supervision_stops_for_create_failed(defer):
    class Parent(Actor):
        def pre_start(self):
            self.child = self.spawn(Child)
            self.watch(self.child)

        def receive(self, message):
            eq_(message, ('terminated', self.child))
            termination_message_received.set()

    class Child(Actor):
        def __init__(self):
            raise MockException
    node = DummyNode()
    defer(node.stop)
    termination_message_received = Event()
    node.spawn(Parent)
    termination_message_received.wait()

    class Child(Actor):
        def pre_start(self):
            raise MockException
    node = DummyNode()
    defer(node.stop)
    termination_message_received = Event()
    node.spawn(Parent)
    termination_message_received.wait()


# def test_TODO_default_supervision_stops_for_actorkilled():
#     # TODO: akka treats actor-killed as an exception--does this make sense?
#     pass


@deferred_cleanup
def test_default_supervision_restarts_by_default(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'fail'

    class Child(Actor):
        def pre_start(self):
            child_started.incr()

        def receive(self, _):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    child_started = obs_count()
    node.spawn(Parent)
    child_started.wait_eq(2)


@deferred_cleanup
def test_supervision_decision_default(defer):
    class Parent(Actor):
        def pre_start(self):
            c = self.spawn(Child)
            child.set(c)
            Events.consume_one(DeadLetter)
            c << 'dummy'

        def supervise(self, _):
            return Default

    class Child(Actor):
        def pre_start(self):
            child_started.incr()
            raise MockException

        def post_stop(self):
            post_stop_called.set()

    node = DummyNode()
    defer(node.stop)

    #

    child = AsyncResult()
    child_started = obs_count()
    post_stop_called = Event()
    node.spawn(Parent)
    child_started.wait_eq(1)
    wait(lambda: child.get().is_stopped)
    ok_(not post_stop_called.is_set(), "actors with failing start up should not have their post_stop called")
    #

    class Child(Actor):
        def pre_start(self):
            child_started.incr()

        def receive(self, _):
            raise MockException

    child_started = obs_count()
    node.spawn(Parent)
    child_started.wait_eq(2)


# def test_TODO_other_error_escalates():
#     # TODO: what does this mean in Akka anyway?
#     pass


@deferred_cleanup
def test_error_is_escalated_if_supervision_returns_escalate_or_nothing(defer):
    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            return supervision_decision

    class Child(Actor):
        def pre_start(self):
            self << 'dummy'

        def receive(self, _):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    supervision_decision = Escalate
    with expect_failure(MockException):
        node.spawn(Supervisor)
    supervision_decision = None
    with expect_failure(MockException):
        node.spawn(Supervisor)


@deferred_cleanup
def test_error_is_escalated_if_supervision_raises_exception(defer):
    class SupervisorException(Exception):
        pass

    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            raise SupervisorException

    class Child(Actor):
        def pre_start(self):
            self << 'dummy'

        def receive(self, _):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    with expect_failure(SupervisorException):
        node.spawn(Supervisor)


@deferred_cleanup
def test_bad_supervision_is_raised_if_supervision_returns_an_illegal_value(defer):
    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            return 'illegal-value'

    class Child(Actor):
        def pre_start(self):
            self << 'dummy'

        def receive(self, _):
            raise MockException

    node = DummyNode()
    defer(node.stop)
    with expect_failure(BadSupervision, "Should raise BadSupervision if supervision returns an illegal value") as basket:
        node.spawn(Supervisor)
        sleep(0.1)
    with assert_raises(MockException):
        basket[0].raise_original()


# def test_TODO_baseexceptions_are_also_propagated_through_the_hierarchy():
#     pass


# def test_TODO_supervise_can_specify_maxrestarts():
#     class Parent(Actor):
#         def supervise(self, _):
#             return Restart(max=3)

#         def pre_start(self):
#             self.spawn(Child)

#     child_started = obs_count()

#     class Child(Actor):
#         def pre_start(self):
#             child_started()
#             raise MockException

#     node.spawn(Parent)

#     assert child_started == 4, child_started


# def test_TODO_supervision_can_be_marked_as_allforone_or_oneforone():
#     pass


##
## GUARDIAN

# def test_TODO_guardian_supervision():
#     pass


##
## HIERARCHY

@deferred_cleanup
def test_actors_remember_their_children(defer):
    class MyActor(Actor):
        def pre_start(self):
            ok_(not self.children)
            child1 = self.spawn(Actor)
            ok_(child1 in self.children)
            child2 = self.spawn(Actor)
            ok_(child2 in self.children)
    node = DummyNode()
    defer(node.stop)
    node.spawn(MyActor)


@deferred_cleanup
def test_stopped_child_is_removed_from_its_parents_list_of_children(defer):
    class MyActor(Actor):
        def pre_start(self):
            child = self.spawn(Actor)
            ok_(child in self.children)
            self.watch(child)
            child.stop()
            # XXX: assert child in self._children

        def receive(self, message):
            receive_called.set()
            eq_(message, ('terminated', ANY))
            _, child = message
            ok_(child not in self.children)

    node = DummyNode()
    defer(node.stop)
    receive_called = Event()
    node.spawn(MyActor)
    receive_called.wait()


@deferred_cleanup
def test_suspending_suspends_and_resuming_resumes_all_children(defer):
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            ok_(isinstance(exc, MockException))
            parent_received_error.set()
            return Ignore  # should resume both Child and SubChild, and allow SubChild to process its message

    class Child(Actor):
        def pre_start(self):
            subchild.set(self.spawn(SubChild))

        def receive(self, message):
            raise MockException  # should also cause SubChild to be suspended

    class SubChild(Actor):
        def receive(self, message):
            subchild_received_message.set()

    node = DummyNode()
    defer(node.stop)
    child, subchild = AsyncResult(), AsyncResult()
    parent_received_error, subchild_received_message = Event(), Event()
    node.spawn(Parent)
    child.get() << 'dummy'  # will cause Child to raise MockException
    subchild.get() << 'dummy'

    parent_received_error.wait()
    subchild_received_message.wait()


@deferred_cleanup
def test_stopping_stops_children(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    node = DummyNode()
    defer(node.stop)
    child_stopped = Event()
    node.spawn(Parent).stop()
    child_stopped.wait()


@deferred_cleanup
def test_stopping_parent_from_child(defer):
    class PoorParent(Actor):
        def pre_start(self):
            child.set(self.spawn(EvilChild))

        def post_stop(self):
            parent_stopped.set()

    class EvilChild(Actor):
        def pre_start(self):
            self._parent.stop()  # this will queue a `_stop` in the parent's queue

    node = DummyNode()
    defer(node.stop)
    child = AsyncResult()
    parent_stopped = Event()
    node.spawn(PoorParent)
    child.get() << 'dummy'
    parent_stopped.wait()


@deferred_cleanup
def test_restarting_stops_children(defer):
    class Parent(Actor):
        def pre_start(self):
            started.incr()
            if started == 1:  # only start the first time, so after the restart, there should be no children
                self.spawn(Child)
            else:
                ok_(not self.children)
                parent_restarted.set()

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    node = DummyNode()
    defer(node.stop)
    started = obs_count()
    child_stopped, parent_restarted = Event(), Event()
    node.spawn(Parent) << '_restart'
    child_stopped.wait()
    parent_restarted.wait()


# def test_TODO_restarting_does_not_restart_children_if_told_so():
#     pass  # Parent.stop_children_on_restart = False


@deferred_cleanup
def test_sending_message_to_stopping_parent_from_post_stop_should_deadletter_the_message(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def receive(self, message):
            ok_(False)

    class Child(Actor):
        def post_stop(self):
            self._parent << 'should-not-be-received'

    node = DummyNode()
    defer(node.stop)
    p = node.spawn(Parent)
    with expect_one_event(DeadLetter(ANY, ANY)):
        p.stop()
        idle()


@deferred_cleanup
def test_queued_messages_are_logged_as_deadletters_after_stop(defer):
    node = DummyNode()
    defer(node.stop)
    deadletter_event_emitted = Events.consume_one(DeadLetter)
    a = node.spawn(Actor)
    a.stop()
    a << 'dummy'
    eq_(deadletter_event_emitted.get(), DeadLetter(a, 'dummy'))


@deferred_cleanup
def test_child_termination_message_from_an_actor_not_a_child_of_the_recipient_is_ignored(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a << ('_child_terminated', node.spawn(Actor))
    idle()


##
## DEATH WATCH

@deferred_cleanup
def test_watch_returns_the_arg(defer):
    class Parent(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            ok_(self.watch(a) is a)
    node = DummyNode()
    defer(node.stop)
    node.spawn(Parent)


@deferred_cleanup
def test_watching_running_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    message_receieved = AsyncResult()
    watchee = node.spawn(Actor)
    node.spawn(Watcher)
    watchee.stop()
    eq_(message_receieved.get(), ('terminated', watchee))


@deferred_cleanup
def test_watching_new_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            a.stop()
            watchee.set(a)
            self.watch(a)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    watchee, message_receieved = AsyncResult(), AsyncResult()
    node.spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee.get()))


@deferred_cleanup
def test_watching_dead_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    message_receieved = AsyncResult()
    watchee = node.spawn(Actor)
    watchee.stop()
    idle()
    node.spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee))


@deferred_cleanup
def test_watching_self_is_noop_and_returns_self(defer):
    class MyActor(Actor):
        def pre_start(self):
            eq_(self.watch(self.ref), self.ref)

        def receive(self, message):
            ok_(False)

    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    dead_letter_emitted = Events.consume_one(DeadLetter)
    a.stop()
    idle()
    ok_(not dead_letter_emitted.ready())


@deferred_cleanup
def test_termination_message_contains_ref_that_forwards_to_deadletters(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', watchee))
            _, sender = message
            with expect_one_event(DeadLetter(sender, 'dummy')):
                sender << 'dummy'
                idle()
            all_ok.set()

    all_ok = Event()
    node = DummyNode()
    defer(node.stop)
    watchee = node.spawn(Actor)
    node.spawn(Watcher)
    watchee.stop()
    all_ok.wait()


@deferred_cleanup
def test_watching_dying_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', ANY))
            message_receieved.set()

    class Watchee(Actor):
        def post_stop(self):
            watchee_released.wait()

    node = DummyNode()
    defer(node.stop)
    watchee_released, message_receieved = Event(), Event()

    watchee = node.spawn(Watchee)
    watchee.stop()
    node.spawn(Watcher)
    watchee_released.set()
    message_receieved.wait()


@deferred_cleanup
def test_unhandled_termination_message_causes_receiver_to_raise_unhandledtermination(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            raise Unhandled

    node = DummyNode()
    defer(node.stop)
    watchee = node.spawn(Actor)
    watchee.stop()
    with expect_failure(UnhandledTermination):
        node.spawn(Watcher)


@deferred_cleanup
def test_system_messages_to_dead_actorrefs_are_discarded(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    for event in ['_stop', '_suspend', '_resume', '_restart']:
        d = Events.consume_one(DeadLetter)
        a << event
        ok_(not d.ready(), "message %r sent to a dead actor should be discarded" % (event,))


@deferred_cleanup
def test_termination_message_to_dead_actor_is_discarded(defer):
    class Parent(Actor):
        def pre_start(self):
            self.watch(self.spawn(Actor)).stop()
            self.stop()
    d = Events.consume_one(DeadLetter)
    node = DummyNode()
    defer(node.stop)
    node.spawn(Parent)
    idle()
    ok_(not d.ready())


@deferred_cleanup
def test_watching_running_remote_actor_that_stops_causes_termination_message(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(self.root.node.lookup_str('localhost:20002/remote-watchee'))

        def receive(self, msg):
            received.set(msg)

    received = AsyncResult()
    node1, node2 = Node('localhost:20001', enable_remoting=True), Node('localhost:20002', enable_remoting=True)
    defer(node1.stop, node2.stop)

    remote_watchee = node2.spawn(Actor, name='remote-watchee')
    node1.spawn(Watcher)
    remote_watchee.stop()

    eq_(received.get(), ('terminated', remote_watchee))


@deferred_cleanup
def test_watching_remote_actor_that_restarts_doesnt_cause_termination_message(defer):
    node1, node2 = Node('localhost:20001', enable_remoting=True), Node('localhost:20002', enable_remoting=True)
    defer(node1.stop, node2.stop)

    received = Event()

    class Watcher(Actor):
        def pre_start(self):
            self.watch(self.root.node.lookup_str('localhost:20002/remote-watchee'))

        def receive(self, msg):
            received.set()

    remote_watchee = node2.spawn(Actor, name='remote-watchee')
    node1.spawn(Watcher)
    sleep(.001)
    remote_watchee << '_restart'

    sleep(.05)
    ok_(not received.is_set())


@deferred_cleanup
def test_watching_nonexistent_remote_actor_causes_termination_message(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, msg):
            received.set(msg)

    received = AsyncResult()
    node1, node2 = (Node('localhost:20001', enable_remoting=True),
                    Node('localhost:20002', enable_remoting=True))
    defer(node1.stop, node2.stop)

    watchee = node1.lookup_str('localhost:20002/nonexistent-watchee')
    node1.spawn(Watcher)

    eq_(received.get(), ('terminated', watchee))


# @simtime
# def test_watching_an_actor_on_a_node_with_whom_connectivity_is_lost_or_limited(clock):
#     def test_it(packet_loss_src, packet_loss_dst):
#         network = MockNetwork(clock)
#         node1, node2 = network.node('watcher-host:123'), network.node('watchee-host:123')

#         received = Event()

#         class Watcher(Actor):
#             def pre_start(self):
#                 self.watchee = self.watch(self.root.node.lookup('watchee-host:123/remote-watchee'))

#             def receive(self, msg):
#                 eq_(msg, ('terminated', self.watchee))
#                 received()
#         node1.spawn(Watcher)

#         node2.spawn(Actor, name='remote-watchee')

#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE / 2.0)
#         ok_(not received)

#         network.packet_loss(100.0,
#                             src='tcp://' + packet_loss_src + '-host:123',
#                             dst='tcp://' + packet_loss_dst + '-host:123')
#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
#         ok_(received)

#     test_it(packet_loss_src='watchee', packet_loss_dst='watcher')
#     test_it(packet_loss_src='watcher', packet_loss_dst='watchee')


##
## REMOTING

@deferred_cleanup
def test_actorref_remote_returns_a_ref_that_when_sent_a_message_delivers_it_on_another_node(defer):
    # This just tests the routing logic and not heartbeat or reliability or deadletters or anything.

    # emulate a scenario in which a single node sends many messages to other nodes;
    # a total of NUM_NODES * NUM_ACTORS messages will be sent out.
    NUM_NODES = 1
    NUM_ACTORS_PER_NODE = 2

    sender_node = Node(nid='localhost:20000', enable_remoting=True)
    defer(sender_node.stop)

    recipient_nodes = []

    for node_ix in range(NUM_NODES):
        nid = 'localhost:2000%d' % (node_ix + 1,)
        remote_node = Node(nid=nid, enable_remoting=True)
        defer(remote_node.stop)

        receive_boxes = []
        sent_msgs = []

        for actor_num in range(1, NUM_ACTORS_PER_NODE + 1):
            actor_box = obs_list()  # collects whatever the MockActor receives
            actor = remote_node.spawn(Props(MockActor, actor_box), name='actor%d' % actor_num)
            # we only care about the messages received, not the ref itself
            receive_boxes.append(actor_box)

            # format: dummy-<nodename>-<actorname>-<random-stuff-for-good-measure> (just for debuggability)
            msg = 'dummy-%s-%s-%s' % (nid, actor.uri.name, random.randint(1, 10000000))
            sender_node.lookup(actor.uri) << msg
            sent_msgs.append(msg)

        recipient_nodes.append((sent_msgs, receive_boxes))

    for sent_msgs, receive_boxes in recipient_nodes:
        for sent_msg, receive_box in zip(sent_msgs, receive_boxes):
            receive_box.wait_eq(terminator=[sent_msg], timeout=None)


def test_transmitting_refs_and_sending_to_received_refs():
    # This just tests the Ref serialisation and deserialization logic.

    @deferred_cleanup
    def test_it(defer, make_actor1):
        node1 = Node(nid='localhost:20001', enable_remoting=True)
        defer(node1.stop)

        actor1_msgs = obs_list()
        actor1 = make_actor1(node1, Props(MockActor, actor1_msgs))

        #
        node2 = Node(nid='localhost:20002', enable_remoting=True)
        defer(node2.stop)

        actor2_msgs = obs_list()
        node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

        # send: node1 -> node2:
        node1.lookup_str('localhost:20002/actor2') << ('msg-with-ref', actor1)

        # reply: node2 -> node1:
        actor2_msgs.wait_eq([ANY], "should be able to send messages to explicitly constructed remote refs")
        _, received_ref = actor2_msgs[0]
        received_ref << ('hello', received_ref)

        actor1_msgs.wait_eq([('hello', received_ref)], "should be able to send messages to received remote refs")

        # send to self without knowing it
        (_, re_received_ref), = actor1_msgs
        del actor1_msgs[:]
        re_received_ref << 'to-myself'

        actor1_msgs.wait_eq(['to-myself'])

    @test_it
    def make_toplevel(node, factory):
        return node.spawn(factory, name='actor1')

    @test_it
    def make_non_toplevel(node, factory):
        class Parent(Actor):
            def pre_start(self):
                child.set(self.spawn(factory, name='actor1'))

        child = AsyncResult()
        node.spawn(Parent, name='parent')
        return child.get()
test_transmitting_refs_and_sending_to_received_refs.timeout = 10


@deferred_cleanup
def test_sending_remote_refs(defer):
    """Sending remote refs.

    The sender acquires a remote ref to an actor on the target and sends it to the sender, who then sends a message.
    to the target. It forms a triangle where 1) M obtains a reference to T, 2) sends it over to S, and then 3) S uses it
    to start communication with T.

    T ---- S
     \   /
      \ /
       M

    """
    target_node = Node('localhost:20003', enable_remoting=True)
    defer(target_node.stop)

    target_msgs = obs_list()
    target_node.spawn(Props(MockActor, target_msgs), name='T')

    #
    sender_node = Node('localhost:20001', enable_remoting=True)
    defer(sender_node.stop)

    class SenderActor(Actor):
        def receive(self, msg):
            eq_(msg, ('send-msg-to', ANY))
            _, target = msg
            target << 'helo'
    sender_node.spawn(SenderActor, name='S')

    #
    middle_node = Node('localhost:20002', enable_remoting=True)
    defer(middle_node.stop)

    ref_to_sender = middle_node.lookup_str('localhost:20001/S')
    ref_to_target = middle_node.lookup_str('localhost:20003/T')
    ref_to_sender << ('send-msg-to', ref_to_target)

    target_msgs.wait_eq(['helo'])


@deferred_cleanup
def test_messages_sent_to_nonexistent_remote_actors_are_deadlettered(defer):
    sender_node, receiver_node = (Node('localhost:20001', enable_remoting=True),
                                  Node('localhost:20002', enable_remoting=True))
    defer(sender_node.stop, receiver_node.stop)

    noexist = sender_node.lookup_str('localhost:20002/non-existent-actor')
    with expect_one_event(RemoteDeadLetter):
        noexist << 'straight-down-the-drain'
test_messages_sent_to_nonexistent_remote_actors_are_deadlettered.timeout = 3.0


## HEARTBEAT

@deferred_cleanup
def test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped(defer):
    sender_node = Node('localhost:20001', enable_remoting=True, hub_kwargs={'heartbeat_interval': 0.05, 'heartbeat_max_silence': 0.1})
    defer(sender_node.stop)
    ref = sender_node.lookup_str('localhost:23456/actor2')
    with expect_one_event(DeadLetter(ref, 'bar')):
        ref << 'bar'
test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped.timeout = 40.0


@deferred_cleanup
def test_sending_to_an_unknown_host_that_becomes_visible_in_time(defer):
    node1 = Node('localhost:20001', enable_remoting=True, hub_kwargs={'heartbeat_interval': 0.05, 'heartbeat_max_silence': 0.5})
    defer(node1.stop)

    ref = node1.lookup_str('localhost:20002/actor1')
    with expect_event_not_emitted(DeadLetter):
        ref << 'foo'

    sleep(0.1)

    node2 = Node('localhost:20002', enable_remoting=True)
    defer(node2.stop)

    actor2_msgs = obs_list()
    node2.spawn(Props(MockActor, actor2_msgs), name='actor1')

    actor2_msgs.wait_eq(['foo'])


# @deferred_cleanup
# def test_sending_stops_if_visibility_is_lost(defer):
#     node1 = Node('host1:123')
#     node2 = Node('host2:123')
#     defer(node1.stop, node2.stop)

#     # set up a normal sending state first

#     ref = node1.lookup_str('host2:123/actor2')
#     ref << 'foo'  # causes host2 to also start the heartbeat

#     sleep(1.0)

#     # ok, now they both know/have seen each other; let's change that:
#     network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)

#     # the next message should fail after 5 seconds
#     ref << 'bar'

#     with expect_one_event(DeadLetter(ref, 'bar')):
#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)


# @simtime
# def test_sending_resumes_if_visibility_is_restored(clock):
#     network = MockNetwork(clock)

#     node1 = network.node('host1:123')

#     network.node('host2:123')

#     ref = node1.lookup('host2:123/actor2')
#     ref << 'foo'
#     network.simulate(duration=1.0)
#     # now both visible to the other

#     network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
#     # now host2 is not visible to host1

#     ref << 'bar'
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE - 1.0)
#     # host1 still hasn't lost faith

#     network.packet_loss(percent=0, src='tcp://host2:123', dst='tcp://host1:123')
#     # and it is rewarded for its patience
#     with expect_event_not_emitted(DeadLetter):
#         network.simulate(duration=2.0)


## REMOTE NAME-TO-PORT MAPPING

# def test_TODO_node_identifiers_are_mapped_to_addresses_on_the_network():
#     pass  # nodes connect to remote mappers on demand


# def test_TODO_node_addresses_are_discovered_automatically_using_a_mapper_daemon_on_each_host():
#     pass  # node registers itself


# def test_TODO_nodes_can_acquire_ports_automatically():
#     pass  # node tries several ports (or asks the mapper?) and settles with the first available one


# def test_TODO_mapper_daemon_can_be_on_a_range_of_ports():
#     pass


## OPTIMIZATIONS

@deferred_cleanup
def test_incoming_refs_pointing_to_local_actors_are_converted_to_local_refs(defer):
    # node1:
    node1 = Node('localhost:20001', enable_remoting=True)
    defer(node1.stop)

    actor1_msgs = obs_list()
    actor1 = node1.spawn(Props(MockActor, actor1_msgs), name='actor1')

    # node2:
    node2 = Node('localhost:20002', enable_remoting=True)
    defer(node2.stop)

    actor2_msgs = obs_list()
    node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

    # send from node1 -> node2:
    node1.lookup_str('localhost:20002/actor2') << ('msg-with-ref', actor1)

    # reply from node2 -> node1:
    _, received_ref = actor2_msgs.wait_eq([ANY])[0]
    received_ref << ('msg-with-ref', received_ref)

    (_, remote_local_ref), = actor1_msgs.wait_eq([ANY])
    ok_(remote_local_ref.is_local)


@deferred_cleanup
def test_looking_up_addresses_that_actually_point_to_the_local_node_return_a_local_ref(defer):
    node = Node('localhost:20000', enable_remoting=True)
    defer(node.stop)
    node.spawn(Actor, name='localactor')
    ref = node.lookup_str('localhost:20000/localactor')
    ok_(ref.is_local)


@deferred_cleanup
def test_sending_to_a_remote_ref_that_points_to_a_local_ref_is_redirected(defer):
    node = Node('localhost:20000', enable_remoting=True)
    defer(node.stop)

    msgs = obs_list()
    node.spawn(Props(MockActor, msgs), name='localactor')

    ref = Ref(cell=None, uri=Uri.parse('localhost:20000/localactor'), is_local=False, node=node)
    ref << 'foo'

    msgs.wait_eq(['foo'])
    ok_(ref.is_local)

    ref << 'bar'
    msgs.wait_eq(['foo', 'bar'])


##
## PROCESSES

@deferred_cleanup
def test_processes_run_is_called_when_the_process_is_spawned(defer):
    class MyProc(Actor):
        def run(self):
            run_called.set()
    node = DummyNode()
    defer(node.stop)
    run_called = Event()
    node.spawn(MyProc)
    run_called.wait()


@deferred_cleanup
def test_warning_is_emitted_if_process_run_returns_a_value(defer):
    class ProcThatReturnsValue(Actor):
        def run(self):
            return 'foo'
    node = DummyNode()
    defer(node.stop)
    with expect_one_warning():
        node.spawn(ProcThatReturnsValue)


def test_process_run_is_paused_and_unpaused_if_the_actor_is_suspended_and_resumed():
    @deferred_cleanup
    def do(defer):
        class MyProc(Actor):
            def run(self):
                released.wait()
                after_release.set()

        released, after_release = Event(), Event()

        node = DummyNode()
        defer(node.stop)
        p = node.spawn(MyProc)
        sleep(0.001)
        ok_(not after_release.is_set())

        p << '_suspend'
        released.set()
        sleep(0.001)
        ok_(not after_release.is_set())

        p << '_resume'
        after_release.wait()
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


@deferred_cleanup
def test_process_run_is_cancelled_if_the_actor_is_stopped(defer):
    class MyProc(Actor):
        def run(self):
            try:
                self.get()
            except GreenletExit:
                exited.set()
    node = DummyNode()
    defer(node.stop)
    exited = Event()
    r = node.spawn(MyProc)
    idle()
    r.stop()
    exited.wait()


@deferred_cleanup
def test_stopping_waits_till_process_is_done_handling_a_message(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            try:
                released.wait()
                self.get()
            except GreenletExit:
                exited.set()
    node = DummyNode()
    defer(node.stop)
    exited, released = Event(), Event()
    r = node.spawn(MyProc)
    r << 'foo'
    sleep(.001)
    r.stop()
    sleep(.001)
    ok_(not exited.is_set())
    released.set()
    exited.wait()


@deferred_cleanup
def test_stopping_a_process_that_never_gets_a_message_just_kills_it_immediately(defer):
    # not the most elegant solution, but there's no other way
    class MyProc(Actor):
        def run(self):
            try:
                Event().wait()
            except GreenletExit:
                exited.set()

        def post_stop(self):
            exited2.set()

    node = DummyNode()
    defer(node.stop)
    exited, exited2 = Event(), Event()
    a = node.spawn(MyProc)
    sleep(.001)
    a.stop()
    exited.wait()
    exited2.wait()


@deferred_cleanup
def test_error_in_stopping_proc_is_ignored(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            released.wait()
            raise MockException
    released = Event()
    node = node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyProc)
    a << 'dummy'
    sleep(.001)
    a.stop()
    with expect_one_event(ErrorIgnored(a, IS_INSTANCE(MockException), ANY)):
        released.set()


@deferred_cleanup
def test_sending_a_message_to_a_process(defer):
    class MyProc(Actor):
        def run(self):
            received_message.set(self.get())
    node = DummyNode()
    defer(node.stop)
    random_message = random.random()
    received_message = AsyncResult()
    node.spawn(MyProc) << random_message
    eq_(received_message.get(), random_message)


@deferred_cleanup
def test_sending_2_messages_to_a_process(defer):
    class MyProc(Actor):
        def run(self):
            first_message.set(self.get())
            second_message.set(self.get())

    node = DummyNode()
    defer(node.stop)
    second_message, first_message = AsyncResult(), AsyncResult()
    msg1, msg2 = random.random(), random.random()
    p = node.spawn(MyProc)
    p << msg1
    eq_(first_message.get(), msg1)
    p << msg2
    eq_(second_message.get(), msg2)


@deferred_cleanup
def test_errors_in_process_while_processing_a_message_are_reported(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            raise MockException
    node = DummyNode()
    defer(node.stop)
    p = node.spawn(MyProc)
    with expect_failure(MockException):
        p << 'dummy'


@deferred_cleanup
def test_error_in_process_suspends_and_taints_and_resuming_it_stops_it(defer):
    class Parent(Actor):
        def supervise(self, exc):
            return Ignore

        def pre_start(self):
            proc.set(self.spawn(MyProc))

    class MyProc(Actor):
        def run(self):
            raise MockException

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    proc = AsyncResult()
    stopped = Event()
    node.spawn(Parent)
    proc.get() << 'dummy'
    stopped.wait()


@deferred_cleanup
def test_restarting_a_process(defer):
    class Parent(Actor):
        def supervise(self, exc):
            return Restart

        def pre_start(self):
            proc.set(self.spawn(MyProc))

    class MyProc(Actor):
        def run(self):
            started.incr()
            self.get()
            raise MockException

    node = DummyNode()
    defer(node.stop)
    proc = AsyncResult()
    started = obs_count()
    node.spawn(Parent)
    started.wait_eq(1)
    proc.get() << 'dummy'
    started.wait_eq(2)


@deferred_cleanup
def test_errors_while_stopping_and_finalizing_are_treated_the_same_as_post_stop_errors(defer):
    class MyProc(Actor):
        def run(self):
            try:
                self.get()
            finally:
                raise MockException
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyProc)
    sleep(.001)
    with expect_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        a.stop()


@deferred_cleanup
def test_all_stashed_messages_are_reported_as_unhandled_on_flush_and_discarded(defer):
    class MyProc(Actor):
        def run(self):
            self.get('dummy')
            self.flush()
            self.get('dummy')
            self.flush()
    node = DummyNode()
    defer(node.stop)
    p = node.spawn(MyProc)
    p << 'should-be-reported-as-unhandled'
    with expect_event_not_emitted(DeadLetter(p, 'should-be-reported-as-unhandled')):
        with expect_one_event(UnhandledMessage(p, 'should-be-reported-as-unhandled')):
            p << 'dummy'
    with expect_event_not_emitted(DeadLetter(p, 'should-be-reported-as-unhandled')):
        with expect_event_not_emitted(UnhandledMessage(p, 'should-be-reported-as-unhandled')):
            p << 'dummy'


@deferred_cleanup
def test_getting_stashed_message(defer):
    class MyProc(Actor):
        def run(self):
            self.get('dummy1')
            self.get('dummy2')
            got_both_messages.set()
            self.get('dummy2')
            ok_(False, "should not reach here")
    node = DummyNode()
    defer(node.stop)
    got_both_messages = Event()
    p = node.spawn(MyProc)
    p << 'dummy2'
    p << 'dummy1'
    got_both_messages.wait()


@deferred_cleanup
def test_dead_letters_are_emitted_in_the_order_the_messages_were_sent(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    with expect_one_event(DeadLetter(a, 'dummy1')):
        with expect_one_event(DeadLetter(a, 'dummy2')):
            a << 'dummy1' << 'dummy2'
            a.stop()


@deferred_cleanup
def test_stashed_messages_are_received_in_the_order_they_were_sent(defer):
    class MyProc(Actor):
        def run(self):
            msgs_received.append(self.get(1))
            msgs_received.append(self.get())
            msgs_received.append(self.get())
            done.set()

    node = DummyNode()
    defer(node.stop)
    msgs_received = []
    done = Event()
    a = node.spawn(MyProc)
    a << 3 << 2 << 1
    done.wait()
    eq_(msgs_received, [1, 3, 2])


@deferred_cleanup
def test_process_is_stopped_when_run_returns(defer):
    class MyProc(Actor):
        def run(self):
            self.get()

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped = Event()
    p = node.spawn(MyProc)
    p << 'dummy'
    stopped.wait()


@deferred_cleanup
def test_process_is_stopped_when_the_coroutine_exits_during_startup(defer):
    class MyProc(Actor):
        def run(self):
            pass

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped = Event()
    node.spawn(MyProc)
    stopped.wait()


@deferred_cleanup
def test_process_can_get_messages_selectively(defer):
    class MyProc(Actor):
        def run(self):
            messages.append(self.get(ANY))
            messages.append(self.get('msg3'))
            messages.append(self.get(ANY))
            release1.wait()
            messages.append(self.get(IS_INSTANCE(int)))
            release2.wait()
            messages.append(self.get(IS_INSTANCE(float)))
    node = DummyNode()
    defer(node.stop)
    messages = obs_list()
    release1, release2 = Event(), Event()
    p = node.spawn(MyProc)
    p << 'msg1'
    messages.wait_eq(['msg1'])
    p << 'msg2'
    messages.wait_eq(['msg1'])
    p << 'msg3'
    messages.wait_eq(['msg1', 'msg3', 'msg2'])
    # (process blocked here)
    p << 'not-an-int'
    release1.set()
    sleep(.001)
    ok_('not-an-int' not in messages)
    p << 123
    sleep(.001)
    ok_(123 in messages)
    # (process blocked here)
    p << 321
    p << 32.1
    release2.set()
    sleep(.001)
    ok_(321 not in messages)
    ok_(32.1 in messages)


# def test_TODO_process_can_delegate_handling_of_caught_exceptions_to_parent():
#     class Parent(Actor):
#         def supervise(self, exc):
#             ok_(isinstance(exc, MockException))
#             supervision_invoked.set()
#             return Restart

#         def pre_start(self):
#             self.spawn(Child) << 'invoke'

#     class Child(Actor):
#         def run(self):
#             self.get()  # put the process into receive mode (i.e. started)
#             try:
#                 raise MockException()
#             except MockException:
#                 self.escalate()
#             process_continued.set()

#     process_continued, supervision_invoked = Event(), Event()
#     node.spawn(Parent)
#     supervision_invoked.wait()
#     ok_(not process_continued.is_set())
#     sleep(.001)


# def test_TODO_calling_escalate_outside_of_error_context_causes_runtime_error():
#     class MyProc(Actor):
#         def supervise(self, exc):
#             ok_(isinstance(exc, InvalidEscalation))
#             exc_raised.set()
#             return Stop

#         def pre_start(self):
#             self.spawn(MyProcChild)

#     class MyProcChild(Actor):
#         def run(self):
#             self << 'foo'
#             self.get()  # put in started-mode
#             self.escalate()

#     node = DummyNode()
#     exc_raised = Event()
#     node.spawn(MyProc)
#     exc_raised.wait()


##
## SUBPROCESS

# def test_TODO_spawning_an_actor_in_subprocess_uses_a_special_agent_guardian():
#     pass  # TODO: subprocess contains another actor system; maybe add a
#           # guardian actor like in Akka, and that guardian could be
#           # different when running in a subprocess and instead of dumping a log message on stderr
#           # it prints a parsable message


# def test_TODO_entire_failing_subproccess_reports_the_subprocessed_actor_as_terminated():
#     # TODO: and how to report  the cause?
#     pass


# def test_TODO_stopping_a_subprocessed_actor_kills_the_subprocess():
#     pass


##
##  MIGRATION

# def test_TODO_migrating_an_actor_to_another_host_suspends_serializes_and_deserializes_it_and_makes_it_look_as_if_it_had_been_deployed_remotely():
#     pass


# def test_TODO_migrating_an_actor_doesnt_break_existing_refs():
#     pass


# def test_TODO_migrating_an_actor_redirects_all_actorrefs_to_it():
#     pass


# def test_TODO_actor_is_allowed_to_fail_to_be_serialized():
#     pass


##
## TYPED ACTORREFS

# def test_TODO_typed_actorrefs():
#     pass


##
## DISPATCHING

# def test_TODO_suspend_and_resume_doesnt_change_global_message_queue_ordering():
#     pass


# SUPPORT

class MockException(Exception):
    pass


def DummyNode():
    return Node()


wrap_globals(globals())
