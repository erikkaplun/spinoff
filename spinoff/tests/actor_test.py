from __future__ import print_function

import gc
import random
import re
import weakref
import time
import sys

from gevent import idle, sleep, GreenletExit
from gevent.event import Event, AsyncResult
from gevent.queue import Channel, Empty
from nose.tools import eq_, ok_

from spinoff.actor import Actor, Props, Node, Ref, Uri
from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, ErrorIgnored, HighWaterMarkReached
from spinoff.actor.process import Process
from spinoff.actor.supervision import Resume, Restart, Stop, Escalate, Default
from spinoff.actor.remoting import Hub, HubWithNoRemoting
from spinoff.actor.remoting.mock import MockNetwork
from spinoff.actor.exceptions import InvalidEscalation, Unhandled, NameConflict, UnhandledTermination, CreateFailed, BadSupervision
from spinoff.util.pattern_matching import ANY, IS_INSTANCE
from spinoff.util.testing import (
    assert_raises, assert_one_warning, swallow_one_warning, MockMessages, assert_one_event, EvSeq,
    EVENT, NEXT, Counter, expect_failure, simtime, MockActor, assert_event_not_emitted,)
from spinoff.actor.events import RemoteDeadLetter
from spinoff.util.testing.actor import wrap_globals
from spinoff.util.logging import dbg


# ATTENTION: all tests functions are smartly auto-wrapped with wrap_globals at the bottom of this file.


##
## SENDING & RECEIVING

def test_sent_message_is_received():
    # Trivial send and receive.
    spawn = DummyNode().spawn
    messages = []
    a = spawn(Props(MockActor, messages))
    a << 'foo'
    idle()
    eq_(messages, ['foo'])


def test_sending_operator_is_chainable():
    # Trivial send and receive using the << operator.
    spawn = DummyNode().spawn
    messages = []
    a = spawn(Props(MockActor, messages))
    a << 'foo' << 'bar'
    idle()
    eq_(messages, ['foo', 'bar'])


def test_receive_of_the_same_actor_never_executes_concurrently():
    # Receive of the same actor is never concurrent.
    #
    # It is guaranteed that an actor that is processing
    # a message (i.e. is not idle), will not receive the next message before processing the previous one completes.
    #
    # This test case rules out only the basic recursive case with a non-switching receive method.
    spawn = DummyNode().spawn

    receive_called = Counter()

    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                receive_called()
                self.ref << None
                eq_(receive_called, 1)
            else:
                receive_called()

    spawn(MyActor) << 'init'
    idle()
    eq_(receive_called, 2)

    #

    receive_called = Counter()

    class MyActor2(Actor):
        def receive(self, message):
            receive_called()
            unblocked.wait()

    unblocked = Event()

    spawn(MyActor2) << None << None
    idle()
    eq_(receive_called, 1)
    unblocked.set()
    idle()
    eq_(receive_called, 2)


def test_unhandled_message_is_reported():
    # Unhandled messages are reported to Events
    spawn = DummyNode().spawn

    class MyActor(Actor):
        def receive(self, _):
            raise Unhandled

    a = spawn(MyActor)
    with assert_one_event(UnhandledMessage(a, 'foo')):
        a << 'foo'
        idle()


def test_unhandled_message_to_guardian_is_also_reported():
    guardian = DummyNode().guardian
    with assert_one_event(UnhandledMessage(guardian, 'foo')):
        guardian << 'foo'
        idle()


def test_with_no_receive_method_all_messages_are_unhandled():
    spawn = DummyNode().spawn
    a = spawn(Actor)
    with assert_one_event(UnhandledMessage(a, 'dummy')):
        a << 'dummy'
        idle()


##
## SPAWNING

def test_spawning():
    spawn = DummyNode().spawn
    init_called, actor_spawned = Event(), Event()

    class MyActor(Actor):
        def __init__(self):
            init_called.set()

        def pre_start(self):
            actor_spawned.set()

    spawn(MyActor)
    ok_(not init_called.is_set())
    ok_(not actor_spawned.is_set())
    actor_spawned.wait()
    ok_(init_called.is_set())


def test_spawning_a_toplevel_actor_assigns_guardian_as_its_parent():
    node = DummyNode()
    spawn = node.spawn
    pre_start_called = Event()

    class MyActor(Actor):
        def pre_start(self):
            ok_(self._parent is node.guardian)
            pre_start_called.set()

    spawn(MyActor)
    pre_start_called.wait()


def test_spawning_child_actor_assigns_the_spawner_as_parent():
    spawn = DummyNode().spawn
    childs_parent = AsyncResult()

    class MyActor(Actor):
        def receive(self, _):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            childs_parent.set(self._parent)

    a = spawn(MyActor)
    a << None
    ok_(childs_parent.get() is a)


def test_spawning_returns_an_immediately_usable_ref():
    spawn = DummyNode().spawn
    other_receive_called = Event()

    class MyActor(Actor):
        def receive(self, _):
            spawn(Other) << 'foo'

    class Other(Actor):
        def receive(self, _):
            other_receive_called.set()

    spawn(MyActor) << None
    other_receive_called.wait(), "Spawned actor should have received a message"


def test_pre_start_is_called_after_constructor_and_ref_and_parent_are_available():
    node = DummyNode()
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


def test_errors_in_pre_start_are_reported_as_create_failed():
    spawn = DummyNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            raise MockException()

    with expect_failure(CreateFailed) as basket:
        spawn(MyActor)
        idle()
    with assert_raises(MockException):
        basket[0].raise_original()


def test_sending_to_self_does_not_deliver_the_message_until_after_the_actor_is_started():
    spawn = DummyNode().spawn

    message_received = Event()

    class MyActor(Actor):
        def pre_start(self):
            self.ref << 'dummy'
            ok_(not message_received.is_set())

        def receive(self, message):
            message_received.set()

    spawn(MyActor)
    idle()
    ok_(message_received.is_set())


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

def test_suspending():
    spawn = DummyNode().spawn

    message_received = Event()

    class MyActor(Actor):
        def receive(self, message):
            message_received.set()

    a = spawn(MyActor)
    a << 'foo'
    message_received.wait()
    message_received.clear()
    a << '_suspend'
    idle()
    ok_(not message_received.is_set())


def test_suspending_while_pre_start_is_blocked_pauses_pre_start():
    def do():
        class MyActor(Actor):
            def pre_start(self):
                released.wait()
                after_release.set()

        spawn = DummyNode().spawn
        released = Event()
        after_release = Event()

        a = spawn(MyActor)
        idle()
        ok_(not after_release.is_set())
        a << '_suspend'
        released.set()
        idle()
        ok_(not after_release.is_set())

        a << '_resume'
        idle()
        ok_(after_release.is_set())
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


def test_suspending_with_nonempty_inbox_while_receive_is_blocked():
    class MyActor(Actor):
        def receive(self, message):
            message_received()
            if not released.is_set():  # only yield the first time for correctness
                released.wait()

    spawn = DummyNode().spawn

    released = Event()
    message_received = Counter()

    a = spawn(MyActor)
    a << None
    idle()
    eq_(message_received, 1)

    a << 'foo'
    a << '_suspend'
    released.set()
    idle()
    eq_(message_received, 1)

    a << '_resume'
    idle()
    eq_(message_received, 2)


def test_suspending_while_receive_is_blocked_pauses_the_receive():
    def do():
        class MyActor(Actor):
            def receive(self, _):
                child_released.wait()
                after_release_reached.set()

        spawn = DummyNode().spawn

        child_released = Event()
        after_release_reached = Event()

        a = spawn(MyActor)
        a << 'dummy'
        idle()
        a << '_suspend'
        child_released.set()
        idle()
        ok_(not after_release_reached.is_set())

        a << '_resume'
        idle()
        ok_(after_release_reached.is_set())
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


def test_suspending_while_already_suspended():
    # This can happen when an actor is suspended and then its parent gets suspended.
    spawn = DummyNode().spawn
    message_received = Event()

    class DoubleSuspendingActor(Actor):
        def receive(self, msg):
            message_received.set()

    a = spawn(DoubleSuspendingActor)
    a << '_suspend' << '_suspend' << '_resume' << 'dummy'
    message_received.wait()


# def test_TODO_stopping():
#     pass


# def test_TODO_force_stopping_does_not_wait_for_a_deferred_post_stop_to_complete():
#     pass


def test_resuming():
    spawn = DummyNode().spawn

    class MyActor(Actor):
        def receive(self, message):
            message_received()

    a = spawn(MyActor)
    a << '_suspend'
    message_received = Counter()
    a << 'foo' << 'foo' << '_resume'
    idle()
    eq_(message_received, 2)


def test_restarting():
    spawn = DummyNode().spawn

    actor_started = Counter()
    message_received = Event()
    post_stop_called = Event()

    class MyActor(Actor):
        def pre_start(self):
            actor_started()

        def receive(self, _):
            message_received.set()

        def post_stop(self):
            post_stop_called.set()

    a = spawn(MyActor)
    idle()
    eq_(actor_started, 1)
    a << '_restart'
    idle()
    ok_(not message_received.is_set())
    eq_(actor_started, 2)
    ok_(post_stop_called.is_set())


def test_restarting_is_not_possible_on_stopped_actors():
    spawn = DummyNode().spawn
    actor_started = Counter()

    class MyActor(Actor):
        def pre_start(self):
            actor_started()

    a = spawn(MyActor)
    idle()
    a.stop()
    idle()
    a << '_restart'
    idle()
    eq_(actor_started, 1)


def test_restarting_doesnt_destroy_the_inbox():
    spawn = DummyNode().spawn

    messages_received = []
    started = Counter()

    class MyActor(Actor):
        def pre_start(self):
            started()

        def receive(self, message):
            messages_received.append(message)

    a = spawn(MyActor)
    a << 'foo'
    a << '_restart'
    a << 'bar'
    idle()

    eq_(messages_received, ['foo', 'bar'])


def test_restarting_waits_till_the_ongoing_receive_is_complete():
    # Restarts are not carried out until the current receive finishes.
    spawn = DummyNode().spawn
    started = Counter()
    deferred_cancelled = Event()
    released = Event()

    class MyActor(Actor):
        def pre_start(self):
            started()

        def receive(self, message):
            if started == 1:
                released.wait()

    a = spawn(MyActor)
    a << 'foo'
    idle()
    a << '_restart'
    ok_(not deferred_cancelled.is_set())
    eq_(started, 1)
    released.set()
    idle()
    eq_(started, 2)


def test_restarting_does_not_complete_until_pre_start_completes():
    spawn = DummyNode().spawn

    released = Event()
    received = Event()
    started = Event()

    class MyActor(Actor):
        def pre_start(self):
            if not started:
                started.set()
            else:
                released.wait()  # only block on restart, not on creation

        def receive(self, message):
            received.set()

    a = spawn(MyActor)

    a << '_restart' << 'dummy'
    idle()
    ok_(not received.is_set())
    released.set()
    received.wait()


def test_actor_is_untainted_after_a_restart():
    spawn = DummyNode().spawn
    started_once = Event()
    received_once = Event()
    child = AsyncResult()
    message_received = Event()

    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            return (Restart if isinstance(exc, CreateFailed) and isinstance(exc.cause, MockException) else
                    Resume if isinstance(exc, MockException) else
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

    spawn(Parent)
    child.get() << 'dummy1'
    received_once.wait()
    child.get() << 'dummy'
    message_received.wait()


def test_stopping_waits_till_the_ongoing_receive_is_complete():
    spawn = DummyNode().spawn
    stopped = Event()
    released = Event()

    class MyActor(Actor):
        def receive(self, message):
            released.wait()

        def post_stop(self):
            stopped.set()

    a = spawn(MyActor) << 'foo'
    idle()
    a.stop()
    ok_(not stopped.is_set())
    released.set()
    idle()
    ok_(stopped.is_set())


def test_messages_sent_by_child_post_stop_to_restarting_parent_are_processed_after_restart():
    spawn = DummyNode().spawn

    parent_started = Counter()
    parent_received = Event()

    class Parent(Actor):
        def pre_start(self):
            parent_started()
            if parent_started == 1:
                self.spawn(Child)

        def receive(self, message):
            eq_(parent_started, 2)
            parent_received.set()

    class Child(Actor):
        def post_stop(self):
            self._parent << 'should-be-received-after-restart'

    spawn(Parent) << '_restart'
    parent_received.wait()


def test_stopping_an_actor_prevents_it_from_processing_any_more_messages():
    spawn = DummyNode().spawn

    class MyActor(Actor):
        def receive(self, _):
            received.set()

    received = Event()
    a = spawn(MyActor)
    a << None
    idle()
    received.wait()
    received.clear()
    a.stop()
    ok_(not received.is_set(), "the '_stop' message should not be receivable in the actor")
    with assert_one_event(DeadLetter(a, None)):
        a << None
        idle()


def test_stopping_calls_post_stop():
    spawn = DummyNode().spawn

    post_stop_called = Event()

    class MyActor(Actor):
        def post_stop(self):
            post_stop_called.set()

    spawn(MyActor).stop()
    post_stop_called.wait()


def test_stopping_waits_for_post_stop():
    spawn = DummyNode().spawn

    class ChildWithDeferredPostStop(Actor):
        def post_stop(self):
            stop_complete.wait()

    stop_complete = Event()
    parent_received = Event()

    class Parent(Actor):
        def pre_start(self):
            self.child = self.watch(self.spawn(ChildWithDeferredPostStop))

        def receive(self, message):
            if message == 'stop-child':
                self.child.stop()
            else:
                parent_received.set()

    a = spawn(Parent)
    a << 'stop-child'
    idle()
    ok_(not parent_received.is_set())
    stop_complete.set()
    parent_received.wait()


def test_parent_is_stopped_before_children():
    spawn = DummyNode().spawn
    parent_stopping, unblocked, child_stopped = Event(), Event(), Event()

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def post_stop(self):
            parent_stopping.set()
            unblocked.wait()

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    a = spawn(Parent)
    a.stop()
    parent_stopping.wait()
    ok_(not child_stopped.is_set())
    unblocked.set()
    child_stopped.wait()


def test_actor_is_not_restarted_until_its_children_are_stopped():
    spawn = DummyNode().spawn
    stop_complete = Event()
    parent_started = Counter()

    class Parent(Actor):
        def pre_start(self):
            parent_started()
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            stop_complete.wait()

    spawn(Parent) << '_restart'
    idle()
    eq_(parent_started, 1)
    stop_complete.set()
    idle()
    eq_(parent_started, 2)


def test_error_reports_to_a_stopping_actor_are_ignored():
    spawn = DummyNode().spawn
    child = AsyncResult()
    child_released = Event()

    class Child(Actor):
        def receive(self, _):
            child_released.wait()
            raise MockException()

    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

    p = spawn(Parent)
    child.get() << 'dummy'
    idle()
    p.stop()
    idle()
    with expect_failure(MockException):
        child_released.set()
        idle()


def test_stopping_in_pre_start_directs_any_refs_to_deadletters():
    spawn = DummyNode().spawn
    message_received = Event()

    class MyActor(Actor):
        def pre_start(self):
            self.stop()

        def receive(self, message):
            dbg(message)
            message_received.set()

    a = spawn(MyActor)
    with assert_one_event(DeadLetter(a, 'dummy')):
        a << 'dummy'
        idle()
    ok_(not message_received.is_set())


def test_stop_message_received_twice_is_ignored():
    spawn = DummyNode().spawn
    a = spawn(Actor)
    a.stop()
    a.stop()
    idle()


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


def test_actors_are_garbage_collected_on_termination():
    spawn = DummyNode().spawn
    del_called = Event()

    class MyActor(Actor):
        def __del__(self):
            del_called.set()

    spawn(MyActor).stop()
    idle()
    gc.collect()
    ok_(del_called.is_set())


def test_cells_are_garbage_collected_on_termination():
    spawn = DummyNode().spawn
    ac = spawn(Actor)
    cell = weakref.ref(ac._cell)
    ok_(cell())
    ac.stop()
    idle()
    gc.collect()
    ok_(not cell())


def test_messages_to_dead_actors_are_sent_to_dead_letters():
    a = DummyNode().spawn(Actor)
    a.stop()
    with assert_one_event(DeadLetter(a, 'should-end-up-as-letter')):
        a << 'should-end-up-as-letter'
        idle()


def test_guardians_path_is_the_root_uri():
    eq_(DummyNode().guardian.uri.path, '')


def test_toplevel_actorrefs_paths_are_prefixed_with_guardians_path():
    spawn = DummyNode().spawn
    eq_(spawn(Actor, name='a').uri.path, '/a')
    eq_(spawn(Actor, name='b').uri.path, '/b')


def test_non_toplevel_actorrefs_are_prefixed_with_their_parents_path():
    class MyActor(Actor):
        def pre_start(self):
            child_ref.set(self.spawn(Actor, name='child'))
    child_ref = AsyncResult()
    a = DummyNode().spawn(MyActor, name='parent')
    eq_(child_ref.get().uri.path, a.uri.path + '/child')


def test_toplevel_actor_paths_must_be_unique():
    spawn = DummyNode().spawn
    spawn(Actor, name='a')
    with assert_raises(NameConflict):
        spawn(Actor, name='a')


def test_non_toplevel_actor_paths_must_be_unique():
    class MyActor(Actor):
        def pre_start(self):
            self.spawn(Actor, name='a')
            with assert_raises(NameConflict):
                self.spawn(Actor, name='a')
            spawned.set()
    spawned = Event()
    DummyNode().spawn(MyActor, name='a')
    spawned.wait()


def test_spawning_toplevel_actors_without_name_assigns_autogenerated_names():
    spawn = DummyNode().spawn
    a = spawn(Actor)
    ok_(re.match(r'/[^/]+$', a.uri.path))
    b = spawn(Actor)
    ok_(re.match(r'/[^/]+$', b.uri.path))
    ok_(b.uri.path != a.uri.path)


def test_spawning_non_toplevel_actors_without_name_assigns_autogenerated_names_with_prefixed_parent_path():
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
    started = Event()
    DummyNode().spawn(MyActor)
    started.wait()


def test_spawning_with_autogenerated_looking_name_raises_an_exception():
    spawn = DummyNode().spawn
    with assert_raises(ValueError):
        spawn(Actor, name='$1')


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

def test_looking_up_an_actor_by_its_absolute_path_returns_the_original_reference_to_it():
    node = DummyNode()
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup('/toplevel') is toplevel_actor)
    ok_(node.lookup(Uri.parse('/toplevel')) is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    ok_(node.lookup('/toplevel/child') is child_actor)
    ok_(node.lookup(Uri.parse('/toplevel/child')) is child_actor)


def test_looking_up_an_actor_by_a_relative_path_returns_the_original_reference_to_it():
    node = DummyNode()
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup('toplevel') is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    ok_(node.lookup('toplevel/child') is child_actor)
    ok_(toplevel_actor / 'child' is child_actor)


def test_looking_up_an_actor_by_a_parent_traversing_relative_path_returns_a_reference_to_it():
    node = DummyNode()

    a = node.spawn(Actor, name='a')
    ok_(node.guardian / 'a' is a)

    b = a._cell.spawn(Actor, name='b')
    ok_(a / 'b' is b)
    ok_(node.guardian / 'a/b' is b)


def test_looking_up_an_absolute_path_as_if_it_were_relative_just_does_an_absolute_lookup():
    node = DummyNode()

    a = node.spawn(Actor, name='a')
    a._cell.spawn(Actor, name='b')
    root_b = node.spawn(Actor, name='b')

    eq_(a / '/b', root_b)


def test_looking_up_a_non_existent_local_actor_raises_runtime_error():
    node = DummyNode()
    with assert_raises(RuntimeError):
        node.guardian / 'noexist'


# def test_looking_up_a_non_existent_local_actor_returns_a_dead_ref_with_nevertheless_correct_uri():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')
#     eq_(node.guardian, node.lookup('local:123'))

#     noexist = node.lookup('local:123/a/b/c')
#     eq_(noexist.uri, 'local:123/a/b/c')
#     ok_(noexist.is_local)
#     with assert_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'

#     hypotheticalchild = noexist / 'hypotheticalchild'
#     ok_(hypotheticalchild.is_local)
#     eq_(hypotheticalchild.uri, 'local:123/a/b/c/hypotheticalchild')
#     with assert_one_event(DeadLetter(hypotheticalchild, 'foo')):
#         hypotheticalchild << 'foo'


# def test_manually_ceated_remote_looking_ref_to_a_non_existend_local_actor_is_converted_to_dead_ref_on_send():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')

#     noexist = Ref(cell=None, is_local=False, uri=Uri.parse('local:123/a/b/c'), hub=node.hub)

#     with assert_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'
#     ok_(noexist.is_local)


##
## SUPERVISION & ERROR HANDLING

def test_supervision_decision_resume():
    """Child is resumed if `supervise` returns `Resume`"""
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise' << 'other-message'

        def supervise(self, _):
            return Resume

    class Child(Actor):
        def receive(self, message):
            if message == 'raise':
                raise MockException
            else:
                message_received.set()

    message_received = Event()
    DummyNode().spawn(Parent)
    message_received.wait()


def test_supervision_decision_restart():
    """Child is restarted if `supervise` returns `Restart`"""
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Restart

    class Child(Actor):
        def pre_start(self):
            child_started()

        def receive(self, message):
            raise MockException

    child_started = Counter()
    DummyNode().spawn(Parent)
    idle()
    eq_(child_started, 2)


def test_supervision_decision_stop():
    """Child is stopped if `supervise` returns `Stop`"""
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

    child_stopped = Event()
    DummyNode().spawn(Parent)
    child_stopped.wait()


def test_supervision_decision_escalate():
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

    escalated = Event()
    DummyNode().spawn(ParentsParent)
    escalated.wait()


# def test_TODO_exception_escalations_are_reported_as_events():
#     pass


def test_error_suspends_actor():
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            ok_(isinstance(exc, MockException))
            parent_received_error.set()
            parent_supervise_released.wait()
            return Resume

    class Child(Actor):
        def receive(self, message):
            if message == 'cause-error':
                raise MockException
            else:
                child_received_message.set()

    spawn = DummyNode().spawn
    child = AsyncResult()
    parent_received_error, parent_supervise_released, child_received_message = Event(), Event(), Event()

    spawn(Parent)
    child.get() << 'cause-error' << 'dummy'
    parent_received_error.wait()
    ok_(not child_received_message.is_set(), "actor should not receive messages after error until being resumed")
    parent_supervise_released.set()
    child_received_message.wait()


def test_exception_after_stop_is_ignored_and_does_not_report_to_parent():
    class Child(Actor):
        def receive(self, _):
            self.stop()
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'dummy'

        def supervise(self, exc):
            ok_(False, "should not reach here")

    spawn = DummyNode().spawn
    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(Parent)
        idle()


def test_error_in_post_stop_prints_but_doesnt_report_to_parent_and_termination_messages_are_sent_as_normal():
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

    spawn = DummyNode().spawn
    termination_message_received = Event()
    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(Parent)
        idle()
    termination_message_received.wait()


def test_supervision_message_is_handled_directly_by_supervise_method():
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

    spawn = DummyNode().spawn
    supervise_called = Event()
    exc_received = AsyncResult()
    exc = MockException('arg1', 'arg2')

    spawn(Parent)
    supervise_called.wait()
    ok_(isinstance(exc_received.get(), MockException))
    ok_(exc_received.get().args, exc.args)


def test_init_error_reports_to_supervisor():
    class ChildWithFailingInit(Actor):
        def __init__(self):
            raise MockException

    class Parent(Actor):
        def supervise(self, exc):
            received_exception.set(exc)
            return Stop

        def pre_start(self):
            self.spawn(ChildWithFailingInit)

    spawn = DummyNode().spawn
    received_exception = AsyncResult()
    spawn(Parent)
    exc = received_exception.get()
    ok_(isinstance(exc, CreateFailed))
    with assert_raises(MockException):
        exc.raise_original()


def test_pre_start_error_reports_to_supervisor():
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

    spawn = DummyNode().spawn
    received_exception = AsyncResult()
    post_stop_called = Event()
    spawn(Parent)
    exc = received_exception.get()
    ok_(isinstance(exc, CreateFailed))
    with assert_raises(MockException):
        exc.raise_original()
    ok_(not post_stop_called.is_set(), "post_stop must not be called if there was an error in pre_start")


def test_receive_error_reports_to_supervisor():
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

    spawn = DummyNode().spawn
    received_exception = AsyncResult()
    spawn(Parent)
    ok_(isinstance(received_exception.get(), MockException),
        "Child errors in the 'receive' method should be sent to its parent")


def test_restarting_or_resuming_an_actor_that_failed_to_init_or_in_pre_start():
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
            child_created()
            if not init_already_raised.is_set():  # avoid infinite ping-pong
                init_already_raised.set()
                raise MockException

    spawn = DummyNode().spawn
    init_already_raised = Event()
    child_created = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Restart))
    idle()
    eq_(child_created, 2)
    #
    init_already_raised = Event()
    child_created = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Resume))
    idle()
    eq_(child_created, 1, "resuming a tainted actor stops it")

    ##

    class ChildWithErrorInPreStart(Actor):
        def pre_start(self):
            child_started()
            if not pre_start_already_raised.is_set():  # avoid infinite ping-pong
                pre_start_already_raised.set()
                raise MockException

    spawn = DummyNode().spawn
    pre_start_already_raised = Event()
    child_started = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Restart))
    idle()
    eq_(child_started, 2)
    #
    pre_start_already_raised = Event()
    child_started = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Resume))
    idle()
    eq_(child_started, 1, "resuming a tainted actor stops it")


def test_error_report_after_restart_is_ignored():
    class Parent(Actor):
        def pre_start(self):
            if not child.ready():  # so that after the restart the child won't exist
                child.set(self.spawn(Child))

    class Child(Actor):
        def receive(self, _):
            child_released.wait()
            raise MockException

    spawn = DummyNode().spawn
    child = AsyncResult()
    child_released = Event()

    parent = spawn(Parent)
    child.get() << 'dummy'
    parent << '_restart'
    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        child_released.set()
        idle()


def test_default_supervision_stops_for_create_failed():
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
    termination_message_received = Event()
    DummyNode().spawn(Parent)
    termination_message_received.wait()

    class Child(Actor):
        def pre_start(self):
            raise MockException
    termination_message_received = Event()
    DummyNode().spawn(Parent)
    termination_message_received.wait()


# def test_TODO_default_supervision_stops_for_actorkilled():
#     # TODO: akka treats actor-killed as an exception--does this make sense?
#     pass


def test_default_supervision_restarts_by_default():
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'fail'

    class Child(Actor):
        def pre_start(self):
            child_started()

        def receive(self, _):
            raise MockException

    child_started = Counter()
    DummyNode().spawn(Parent)
    idle()
    eq_(child_started, 2)


def test_supervision_decision_default():
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
            child_started()
            raise MockException

    spawn = DummyNode().spawn

    #

    child = AsyncResult()
    child_started = Counter()
    spawn(Parent)
    idle()
    eq_(child_started, 1)
    ok_(child.get().is_stopped)

    #

    class Child(Actor):
        def pre_start(self):
            child_started()

        def receive(self, _):
            raise MockException

    child_started = Counter()
    spawn(Parent)
    idle()
    eq_(child_started, 2)


# def test_TODO_other_error_escalates():
#     # TODO: what does this mean in Akka anyway?
#     pass


def test_error_is_escalated_if_supervision_returns_escalate_or_nothing():
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

    spawn = DummyNode().spawn
    supervision_decision = Escalate
    with expect_failure(MockException):
        spawn(Supervisor)
        idle()
    supervision_decision = None
    with expect_failure(MockException):
        spawn(Supervisor)
        idle()


def test_error_is_escalated_if_supervision_raises_exception():
    spawn = DummyNode().spawn

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

    with expect_failure(SupervisorException):
        spawn(Supervisor)
        idle()


def test_bad_supervision_is_raised_if_supervision_returns_an_illegal_value():
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

    spawn = DummyNode().spawn
    with expect_failure(BadSupervision, "Should raise BadSupervision if supervision returns an illegal value") as basket:
        spawn(Supervisor)
        idle()
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

#     child_started = Counter()

#     class Child(Actor):
#         def pre_start(self):
#             child_started()
#             raise MockException

#     spawn(Parent)

#     assert child_started == 4, child_started


# def test_TODO_supervision_can_be_marked_as_allforone_or_oneforone():
#     pass


##
## GUARDIAN

# def test_TODO_guardian_supervision():
#     pass


##
## HIERARCHY

def test_actors_remember_their_children():
    class MyActor(Actor):
        def pre_start(self):
            ok_(not self.children)
            child1 = self.spawn(Actor)
            ok_(child1 in self.children)
            child2 = self.spawn(Actor)
            ok_(child2 in self.children)
    DummyNode().spawn(MyActor)


def test_stopped_child_is_removed_from_its_parents_list_of_children():
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

    receive_called = Event()
    DummyNode().spawn(MyActor)
    receive_called.wait()


def test_suspending_suspends_and_resuming_resumes_all_children():
    class Parent(Actor):
        def pre_start(self):
            child.set(self.spawn(Child))

        def supervise(self, exc):
            ok_(isinstance(exc, MockException))
            parent_received_error.set()
            return Resume  # should resume both Child and SubChild, and allow SubChild to process its message

    class Child(Actor):
        def pre_start(self):
            subchild.set(self.spawn(SubChild))

        def receive(self, message):
            raise MockException  # should also cause SubChild to be suspended

    class SubChild(Actor):
        def receive(self, message):
            subchild_received_message.set()

    spawn = DummyNode().spawn
    child, subchild = AsyncResult(), AsyncResult()
    parent_received_error, subchild_received_message = Event(), Event()
    spawn(Parent)
    child.get() << 'dummy'  # will cause Child to raise MockException
    subchild.get() << 'dummy'

    parent_received_error.wait()
    subchild_received_message.wait()


def test_stopping_stops_children():
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    spawn = DummyNode().spawn
    child_stopped = Event()
    spawn(Parent).stop()
    child_stopped.wait()


def test_stopping_parent_from_child():
    class PoorParent(Actor):
        def pre_start(self):
            child.set(self.spawn(EvilChild))

        def post_stop(self):
            parent_stopped.set()

    class EvilChild(Actor):
        def pre_start(self):
            self._parent.stop()  # this will queue a `_stop` in the parent's queue

    child = AsyncResult()
    parent_stopped = Event()
    DummyNode().spawn(PoorParent)
    child.get() << 'dummy'
    parent_stopped.wait()


def test_restarting_stops_children():
    class Parent(Actor):
        def pre_start(self):
            started()
            if started == 1:  # only start the first time, so after the restart, there should be no children
                self.spawn(Child)
            else:
                ok_(not self.children)
                parent_restarted.set()

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    started = Counter()
    child_stopped, parent_restarted = Event(), Event()
    DummyNode().spawn(Parent) << '_restart'
    child_stopped.wait()
    parent_restarted.wait()


# def test_TODO_restarting_does_not_restart_children_if_told_so():
#     pass  # Parent.stop_children_on_restart = False


def test_sending_message_to_stopping_parent_from_post_stop_should_deadletter_the_message():
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def receive(self, message):
            ok_(False)

    class Child(Actor):
        def post_stop(self):
            self._parent << 'should-not-be-received'

    p = DummyNode().spawn(Parent)
    with assert_one_event(DeadLetter(ANY, ANY)):
        p.stop()
        idle()


def test_queued_messages_are_logged_as_deadletters_after_stop():
    spawn = DummyNode().spawn
    deadletter_event_emitted = Events.consume_one(DeadLetter)
    a = spawn(Actor)
    a.stop()
    a << 'dummy'
    eq_(deadletter_event_emitted.get(), DeadLetter(a, 'dummy'))


def test_child_termination_message_from_an_actor_not_a_child_of_the_recipient_is_ignored():
    spawn = DummyNode().spawn
    a = spawn(Actor)
    a << ('_child_terminated', spawn(Actor))
    idle()


##
## DEATH WATCH

def test_watch_returns_the_arg():
    class Parent(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            ok_(self.watch(a) is a)
    DummyNode().spawn(Parent)


def test_watching_running_actor():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    spawn = DummyNode().spawn
    message_receieved = AsyncResult()
    watchee = spawn(Actor)
    spawn(Watcher)
    watchee.stop()
    eq_(message_receieved.get(), ('terminated', watchee))


def test_watching_new_actor():
    class Watcher(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            a.stop()
            watchee.set(a)
            self.watch(a)

        def receive(self, message):
            message_receieved.set(message)

    spawn = DummyNode().spawn
    watchee, message_receieved = AsyncResult(), AsyncResult()
    spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee.get()))


def test_watching_dead_actor():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    spawn = DummyNode().spawn
    message_receieved = AsyncResult()
    watchee = spawn(Actor)
    watchee.stop()
    idle()
    spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee))


def test_watching_self_is_noop_and_returns_self():
    class MyActor(Actor):
        def pre_start(self):
            eq_(self.watch(self.ref), self.ref)

        def receive(self, message):
            ok_(False)

    a = DummyNode().spawn(MyActor)
    dead_letter_emitted = Events.consume_one(DeadLetter)
    a.stop()
    idle()
    ok_(not dead_letter_emitted.ready())


def test_termination_message_contains_ref_that_forwards_to_deadletters():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', watchee))
            _, sender = message
            with assert_one_event(DeadLetter(sender, 'dummy')):
                sender << 'dummy'
                idle()
            all_ok.set()

    all_ok = Event()
    spawn = DummyNode().spawn
    watchee = spawn(Actor)
    spawn(Watcher)
    watchee.stop()
    all_ok.wait()


def test_watching_dying_actor():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', ANY))
            message_receieved.set()

    class Watchee(Actor):
        def post_stop(self):
            watchee_released.wait()

    spawn = DummyNode().spawn
    watchee_released, message_receieved = Event(), Event()

    watchee = spawn(Watchee)
    watchee.stop()
    spawn(Watcher)
    watchee_released.set()
    message_receieved.wait()


def test_unhandled_termination_message_causes_receiver_to_raise_unhandledtermination():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            raise Unhandled

    spawn = DummyNode().spawn
    watchee = spawn(Actor)
    watchee.stop()
    with expect_failure(UnhandledTermination):
        spawn(Watcher)
        idle()


def test_system_messages_to_dead_actorrefs_are_discarded():
    a = DummyNode().spawn(Actor)
    a.stop()
    for event in ['_stop', '_suspend', '_resume', '_restart']:
        d = Events.consume_one(DeadLetter)
        a << event
        ok_(not d.ready(), "message %r sent to a dead actor should be discarded" % (event,))


def test_termination_message_to_dead_actor_is_discarded():
    class Parent(Actor):
        def pre_start(self):
            self.watch(self.spawn(Actor)).stop()
            self.stop()
    d = Events.consume_one(DeadLetter)
    DummyNode().spawn(Parent)
    idle()
    ok_(not d.ready())


# @simtime
# def test_watching_running_remote_actor_that_stops_causes_termination_message(clock):
#     network = MockNetwork(clock)
#     node1, node2 = network.node('host1:123'), network.node('host2:123')

#     received = Event()

#     class Watcher(Actor):
#         def pre_start(self):
#             self.watchee = self.watch(self.root.node.lookup('host2:123/remote-watchee'))

#         def receive(self, msg):
#             eq_(msg, ('terminated', self.watchee))
#             received()
#     node1.spawn(Watcher)

#     remote_watchee = node2.spawn(Actor, name='remote-watchee')

#     network.simulate(duration=1.0)
#     assert not received

#     remote_watchee.stop()

#     network.simulate(duration=1.0)
#     assert received


# @simtime
# def test_watching_remote_actor_that_restarts_doesnt_cause_termination_message(clock):
#     network = MockNetwork(clock)
#     node1, node2 = network.node('host1:123'), network.node('host2:123')

#     received = Event()

#     class Watcher(Actor):
#         def pre_start(self):
#             self.watchee = self.watch(self.root.node.lookup('host2:123/remote-watchee'))

#         def receive(self, msg):
#             eq_(msg, ('terminated', self.watchee))
#             received()
#     node1.spawn(Watcher)

#     remote_watchee = node2.spawn(Actor, name='remote-watchee')

#     network.simulate(duration=1.0)
#     assert not received

#     remote_watchee << '_restart'

#     network.simulate(duration=1.0)
#     assert not received


# @simtime
# def test_watching_nonexistent_remote_actor_causes_termination_message(clock):
#     network = MockNetwork(clock)
#     node1, _ = network.node('host1:123'), network.node('host2:123')

#     received = Event()

#     class Watcher(Actor):
#         def pre_start(self):
#             self.watchee = self.watch(self.root.node.lookup('host2:123/nonexistent-watchee'))

#         def receive(self, msg):
#             eq_(msg, ('terminated', self.watchee))
#             received()
#     node1.spawn(Watcher)

#     network.simulate(duration=2.0)
#     assert received


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
#         assert not received

#         network.packet_loss(100.0,
#                             src='tcp://' + packet_loss_src + '-host:123',
#                             dst='tcp://' + packet_loss_dst + '-host:123')
#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
#         assert received

#     test_it(packet_loss_src='watchee', packet_loss_dst='watcher')
#     test_it(packet_loss_src='watcher', packet_loss_dst='watchee')


# ##
# ## REMOTING

# @simtime
# def test_actorref_remote_returns_a_ref_that_when_sent_a_message_delivers_it_on_another_node(clock):
#     # This just tests the routing logic and not heartbeat or reliability or deadletters or anything.

#     # emulate a scenario in which a single node sends many messages to other nodes;
#     # a total of NUM_NODES * NUM_ACTORS messages will be sent out.
#     NUM_NODES = 3
#     NUM_ACTORS_PER_NODE = 20

#     network = MockNetwork(clock)

#     sender_node = network.node('senderhost:123')
#     assert not network.queue

#     recipient_nodes = []

#     for node_num in range(1, NUM_NODES + 1):
#         nodeaddr = 'recphost%d:123' % node_num
#         remote_node = network.node(nodeaddr)

#         receive_boxes = []
#         sent_msgs = []

#         for actor_num in range(1, NUM_ACTORS_PER_NODE + 1):
#             actor_box = []  # collects whatever the MockActor receives
#             actor = remote_node.spawn(Props(MockActor, actor_box), name='actor%d' % actor_num)
#             # we only care about the messages received, not the ref itself
#             receive_boxes.append(actor_box)

#             # format: dummy-<nodename>-<actorname>-<random-stuff-for-good-measure> (just for debuggability)
#             msg = 'dummy-%s-%s-%s' % (nodeaddr, actor.uri.name, random.randint(1, 10000000))
#             sender_node.lookup(actor.uri) << msg
#             sent_msgs.append(msg)

#         recipient_nodes.append((sent_msgs, receive_boxes))

#     random.shuffle(network.queue)  # for good measure
#     network.simulate(duration=3.0)

#     for sent_msgs, receive_boxes in recipient_nodes:
#         all(eq_(receive_box, [sent_msg])
#             for sent_msg, receive_box in zip(sent_msgs, receive_boxes))


# def test_transmitting_refs_and_sending_to_received_refs():
#     # This just tests the Ref serialisation and deserialization logic.

#     @simtime
#     def test_it(clock, make_actor1):
#         network = MockNetwork(clock)

#         #
#         node1 = network.node('host1:123')

#         actor1_msgs = MockMessages()
#         actor1 = make_actor1(node1, Props(MockActor, actor1_msgs))

#         #
#         node2 = network.node('host2:123')

#         actor2_msgs = []
#         node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

#         # send: node1 -> node2:
#         node1.lookup('host2:123/actor2') << ('msg-with-ref', actor1)

#         network.simulate(duration=2.0)

#         # reply: node2 -> node1:
#         assert actor2_msgs == [ANY], "should be able to send messages to explicitly constructed remote refs"
#         _, received_ref = actor2_msgs[0]
#         received_ref << ('hello', received_ref)

#         network.simulate(duration=2.0)
#         assert actor1_msgs == [('hello', received_ref)], "should be able to send messages to received remote refs"

#         # send to self without knowing it
#         (_, re_received_ref), = actor1_msgs
#         del actor1_msgs[:]
#         re_received_ref << 'to-myself'

#         assert actor1_msgs == ['to-myself']

#     @test_it
#     def make_toplevel(node, factory):
#         return node.spawn(factory, name='actor1')

#     @test_it
#     def make_non_toplevel(node, factory):
#         class Parent(Actor):
#             def pre_start(self):
#                 child << self.spawn(factory, name='actor1')

#         child = AsyncResult()
#         node.spawn(Parent, name='parent')
#         return child()


# @simtime
# def test_sending_remote_refs(clock):
#     """Sending remote refs.

#     The sender acquires a remote ref to an actor on the target and sends it to the sender, who then sends a message.
#     to the target. It forms a triangle where 1) M obtains a reference to T, 2) sends it over to S, and then 3) S uses it
#     to start communication with T.

#     T ---- S
#      \   /
#       \ /
#        M

#     """
#     network = MockNetwork(clock)

#     #
#     target_node = network.node('target:123')
#     target_msgs = []
#     target_node.spawn(Props(MockActor, target_msgs), name='T')

#     #
#     sender_node = network.node('sender:123')

#     class SenderActor(Actor):
#         def receive(self, msg):
#             assert msg == ('send-msg-to', ANY)
#             _, target = msg
#             target << 'helo'
#     sender_node.spawn(SenderActor, name='S')

#     #
#     middle_node = network.node('middle:123')
#     ref_to_sender = middle_node.lookup('sender:123/S')
#     ref_to_target = middle_node.lookup('target:123/T')
#     ref_to_sender << ('send-msg-to', ref_to_target)

#     network.simulate(duration=1.0)

#     eq_(target_msgs, ['helo'])


# @simtime
# def test_messages_sent_to_nonexistent_remote_actors_are_deadlettered(clock):
#     network = MockNetwork(clock)

#     sender_node, _ = network.node('sendernode:123'), network.node('receivernode:123')

#     noexist = sender_node.lookup('receivernode:123/non-existent-actor')
#     noexist << 'straight-down-the-drain'
#     with assert_one_event(RemoteDeadLetter):
#         network.simulate(0.2)


# ## HEARTBEAT

# @simtime
# def test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped(clock):
#     network = MockNetwork(clock)

#     sender_node = network.node('sender:123')

#     # recipient host not reachable within time limit--message dropped after `QUEUE_ITEM_LIFETIME`

#     ref = sender_node.lookup('nonexistenthost:123/actor2')
#     ref << 'bar'

#     with assert_one_event(DeadLetter(ref, 'bar')):
#         network.simulate(sender_node.hub.HEARTBEAT_MAX_SILENCE + 1.0)


# @simtime
# def test_sending_to_an_unknown_host_that_becomes_visible_in_time(clock):
#     network = MockNetwork(clock)

#     node1 = network.node('host1:123')
#     node1.hub.QUEUE_ITEM_LIFETIME = 10.0

#     # recipient host reachable within time limit

#     ref = node1.lookup('host2:123/actor1')
#     ref << 'foo'
#     with assert_event_not_emitted(DeadLetter):
#         network.simulate(duration=1.0)

#     node2 = network.node('host2:123')
#     actor2_msgs = []
#     node2.spawn(Props(MockActor, actor2_msgs), name='actor1')

#     network.simulate(duration=3.0)

#     assert actor2_msgs == ['foo']


# @simtime
# def test_sending_stops_if_visibility_is_lost(clock):
#     network = MockNetwork(clock)

#     node1 = network.node('host1:123')

#     network.node('host2:123')

#     # set up a normal sending state first

#     ref = node1.lookup('host2:123/actor2')
#     ref << 'foo'  # causes host2 to also start the heartbeat

#     network.simulate(duration=1.0)

#     # ok, now they both know/have seen each other; let's change that:
#     network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)

#     # the next message should fail after 5 seconds
#     ref << 'bar'

#     with assert_one_event(DeadLetter(ref, 'bar')):
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
#     with assert_event_not_emitted(DeadLetter):
#         network.simulate(duration=2.0)


# ## REMOTE NAME-TO-PORT MAPPING

# def test_TODO_node_identifiers_are_mapped_to_addresses_on_the_network():
#     pass  # nodes connect to remote mappers on demand


# def test_TODO_node_addresses_are_discovered_automatically_using_a_mapper_daemon_on_each_host():
#     pass  # node registers itself


# def test_TODO_nodes_can_acquire_ports_automatically():
#     pass  # node tries several ports (or asks the mapper?) and settles with the first available one


# def test_TODO_mapper_daemon_can_be_on_a_range_of_ports():
#     pass


# ## OPTIMIZATIONS

# @simtime
# def test_incoming_refs_pointing_to_local_actors_are_converted_to_local_refs(clock):
#     network = MockNetwork(clock)

#     # node1:

#     node1 = network.node('host1:123')

#     actor1_msgs = MockMessages()
#     actor1 = node1.spawn(Props(MockActor, actor1_msgs), name='actor1')

#     # node2:

#     node2 = network.node('host2:123')
#     # guardian2 = Guardian(hub=hub2)

#     actor2_msgs = []
#     node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

#     # send from node1 -> node2:
#     node1.lookup('host2:123/actor2') << ('msg-with-ref', actor1)
#     network.simulate(duration=2.0)

#     # reply from node2 -> node1:
#     _, received_ref = actor2_msgs[0]
#     received_ref << ('msg-with-ref', received_ref)

#     network.simulate(duration=2.0)
#     (_, remote_local_ref), = actor1_msgs
#     assert remote_local_ref.is_local


# @simtime
# def test_looking_up_addresses_that_actually_point_to_the_local_node_return_a_local_ref(clock):
#     node = MockNetwork(clock).node('localhost:123')
#     node.spawn(Actor, name='localactor')
#     ref = node.lookup('localhost:123/localactor')
#     assert ref.is_local


# @simtime
# def test_sending_to_a_remote_ref_that_points_to_a_local_ref_is_redirected(clock):
#     network = MockNetwork(clock)
#     node = network.node('localhost:123')

#     msgs = []
#     node.spawn(Props(MockActor, msgs), name='localactor')

#     ref = Ref(cell=None, uri=Uri.parse('localhost:123/localactor'), is_local=False, hub=node.hub)
#     ref << 'foo'

#     network.simulate(5.0)
#     assert msgs == ['foo']
#     assert ref.is_local

#     ref << 'bar'
#     # no network.simulate should be needed
#     assert msgs == ['foo', 'bar']


# # ZMQ

# def test_remoting_with_real_zeromq():
#     from nose.twistedtools import reactor
#     from txzmq import ZmqFactory, ZmqPushConnection, ZmqPullConnection

#     class MyActor(Actor):
#         def __init__(self, msgs, triggers):
#             self.msgs = msgs
#             self.triggers = triggers

#         def receive(self, msg):
#             self.msgs.append(msg)
#             self.triggers.pop(0)()

#     f1 = ZmqFactory()
#     insock = ZmqPullConnection(f1)
#     outsock_factory = lambda: ZmqPushConnection(f1, linger=0)
#     node1 = Node(hub=Hub(insock, outsock_factory, '127.0.0.1:19501'))

#     f2 = ZmqFactory()
#     insock = ZmqPullConnection(f2, linger=0)
#     outsock_factory = lambda: ZmqPushConnection(f2, linger=0)
#     node2 = Node(hub=Hub(insock, outsock_factory, '127.0.0.1:19502'))

#     yield sleep(0.001)

#     actor1_msgs, actor1_triggers = MockMessages(), [Barrier(), Barrier(), Barrier()]
#     actor1 = node1.spawn(Props(MyActor, actor1_msgs, actor1_triggers), 'actor1')

#     actor2_msgs, actor2_triggers = MockMessages(), [Barrier(), Barrier(), Barrier()]
#     node2.spawn(Props(MyActor, actor2_msgs, actor2_triggers), 'actor2')

#     # simple message: @node1 => actor2@node2
#     actor2_from_node1 = node1.lookup('127.0.0.1:19502/actor2')

#     actor2_from_node1 << 'helloo!'
#     yield actor2_triggers[0]
#     eq_(actor2_msgs.clear(), ['helloo!'])

#     # message containing actor1's ref: @node1 => actor2@node2
#     actor2_from_node1 << actor1

#     yield actor2_triggers[0]
#     tmp = actor2_msgs.clear()
#     eq_(tmp, [actor1])
#     actor1_from_node2 = tmp[0]

#     # message to received actor1's ref: @node2 => actor1@node1
#     actor1_from_node2 << 'helloo2!'

#     yield actor1_triggers[0]
#     eq_(actor1_msgs.clear(), ['helloo2!'])

#     # message containing the received actor1's ref to actor1 (via the same ref): @node2 => actor1@node1
#     actor1_from_node2 << ('msg-with-ref', actor1_from_node2)

#     yield actor1_triggers[0]
#     tmp = actor1_msgs.clear()
#     eq_(tmp, [('msg-with-ref', actor1)])
#     _, self_ref = tmp[0]
#     ok_(self_ref.is_local)
#     ok_(self_ref, actor1)

#     self_ref << 'hello, stranger!'
#     eq_(actor1_msgs.clear(), ['hello, stranger!'])
# test_remoting_with_real_zeromq.timeout = 2.0


##
## PROCESSES


def test_processes_run_is_called_when_the_process_is_spawned():
    class MyProc(Process):
        def run(self):
            run_called.set()
    run_called = Event()
    DummyNode().spawn(MyProc)
    run_called.wait()


def test_warning_is_emitted_if_process_run_returns_a_value():
    class ProcThatReturnsValue(Process):
        def run(self):
            return 'foo'
    spawn = DummyNode().spawn
    with assert_one_warning():
        spawn(ProcThatReturnsValue)
        idle()


def test_process_run_is_paused_and_unpaused_if_the_actor_is_suspended_and_resumed():
    def do():
        class MyProc(Process):
            def run(self):
                released.wait()
                after_release.set()

        released, after_release = Event(), Event()

        p = DummyNode().spawn(MyProc)
        idle()
        ok_(not after_release.is_set())

        p << '_suspend'
        released.set()
        idle()
        ok_(not after_release.is_set())

        p << '_resume'
        after_release.wait()
    # TODO: not possible to implement until gevent/greenlet acquires a means to suspend greenlets
    with assert_raises(AssertionError):
        do()


def test_process_run_is_cancelled_if_the_actor_is_stopped():
    class MyProc(Process):
        def run(self):
            try:
                self.get()
            except GreenletExit:
                exited.set()
    exited = Event()
    r = DummyNode().spawn(MyProc)
    idle()  # `run` is not otherwise called if we .stop() immediately
    r.stop()
    exited.wait()


def test_sending_a_message_to_a_process():
    class MyProc(Process):
        def run(self):
            received_message.set(self.get())
    random_message = random.random()
    received_message = AsyncResult()
    DummyNode().spawn(MyProc) << random_message
    eq_(received_message.get(), random_message)


def test_sending_2_messages_to_a_process():
    class MyProc(Process):
        def run(self):
            first_message.set(self.get())
            second_message.set(self.get())

    second_message, first_message = AsyncResult(), AsyncResult()
    msg1, msg2 = random.random(), random.random()
    p = DummyNode().spawn(MyProc)
    p << msg1
    eq_(first_message.get(), msg1)
    p << msg2
    eq_(second_message.get(), msg2)


def test_errors_in_process_while_processing_a_message_are_reported():
    class MyProc(Process):
        def run(self):
            self.get()
            raise MockException
    p = DummyNode().spawn(MyProc)
    with expect_failure(MockException):
        p << 'dummy'
        idle()


def test_error_in_process_suspends_and_taints_and_resuming_it_stops_it():
    class Parent(Actor):
        def supervise(self, exc):
            return Resume

        def pre_start(self):
            proc.set(self.spawn(MyProc))

    class MyProc(Process):
        def run(self):
            raise MockException

        def post_stop(self):
            stopped.set()

    proc = AsyncResult()
    stopped = Event()
    DummyNode().spawn(Parent)
    proc.get() << 'dummy'
    stopped.wait()


def test_restarting_a_process_reinvokes_its_run_method():
    class Parent(Actor):
        def supervise(self, exc):
            return Restart

        def pre_start(self):
            proc.set(self.spawn(MyProc))

    class MyProc(Process):
        def run(self):
            started()
            self.get()
            raise MockException

    proc = AsyncResult()
    started = Counter()
    DummyNode().spawn(Parent)
    idle()
    eq_(started, 1)
    proc.get() << 'dummy'
    idle()
    eq_(started, 2)


# def test_errors_while_stopping_and_finalizing_are_treated_the_same_as_post_stop_errors():
#     spawn = DummyNode().spawn

#     class MyProc(Process):
#         def run(self):
#             try:
#                 yield self.get()
#             finally:
#                 # this also covers the process trying to yield stuff, e.g. for getting another message
#                 raise MockException

#     with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
#         spawn(MyProc).stop()


# def test_all_queued_messages_are_reported_as_unhandled_on_flush():
#     spawn = DummyNode().spawn

#     released = Event()

#     class MyProc(Process):
#         def run(self):
#             yield self.get()
#             yield released
#             self.flush()

#     p = spawn(MyProc)
#     p << 'dummy'
#     p << 'should-be-reported-as-unhandled'
#     with assert_one_event(UnhandledMessage(p, 'should-be-reported-as-unhandled')):
#         released()


# def test_process_is_stopped_when_the_coroutine_exits():
#     spawn = DummyNode().spawn

#     class MyProc(Process):
#         def run(self):
#             yield self.get()

#     p = spawn(MyProc)
#     p << 'dummy'
#     yield p.join()


# def test_process_is_stopped_when_the_coroutine_exits_during_startup():
#     spawn = DummyNode().spawn

#     class MyProc(Process):
#         def run(self):
#             yield

#     p = spawn(MyProc)
#     yield p.join()


# def test_process_can_get_messages_selectively():
#     spawn = DummyNode().spawn

#     messages = []

#     release1 = Event()
#     release2 = Event()

#     class MyProc(Process):
#         def run(self):
#             messages.append((yield self.get(ANY)))
#             messages.append((yield self.get('msg3')))
#             messages.append((yield self.get(ANY)))

#             yield release1

#             messages.append((yield self.get(IS_INSTANCE(int))))

#             yield release2

#             messages.append((yield self.get(IS_INSTANCE(float))))

#     p = spawn(MyProc)
#     p << 'msg1'
#     eq_(messages, ['msg1'])

#     p << 'msg2'
#     eq_(messages, ['msg1'])

#     p << 'msg3'
#     eq_(messages, ['msg1', 'msg3', 'msg2'])

#     # (process blocked here)

#     p << 'not-an-int'
#     release1()
#     assert 'not-an-int' not in messages

#     p << 123
#     assert 123 in messages

#     # (process blocked here)

#     p << 321
#     p << 32.1

#     release2()
#     assert 321 not in messages and 32.1 in messages


# def test_process_can_delegate_handling_of_caught_exceptions_to_parent():
#     spawn = DummyNode().spawn

#     process_continued = Event()
#     supervision_invoked = Event()

#     class Parent(Actor):
#         def supervise(self, exc):
#             assert isinstance(exc, MockException)
#             supervision_invoked()
#             return Restart

#         def pre_start(self):
#             self.spawn(Child) << 'invoke'

#     class Child(Process):
#         def run(self):
#             yield self.get()  # put the process into receive mode (i.e. started)
#             try:
#                 raise MockException()
#             except MockException:
#                 yield self.escalate()
#             process_continued()

#     spawn(Parent)

#     yield supervision_invoked
#     assert not process_continued
#     yield sleep(0)


# def test_calling_escalate_outside_of_error_context_causes_runtime_error():
#     spawn = DummyNode().spawn

#     exc_raised = Event()

#     class FalseAlarmParent(Actor):
#         def supervise(self, exc):
#             ok_(isinstance(exc, InvalidEscalation))
#             exc_raised()
#             return Stop

#         def pre_start(self):
#             self.spawn(FalseAlarmChild)

#     class FalseAlarmChild(Process):
#         def run(self):
#             self << 'foo'
#             yield self.get()  # put in started-mode
#             self.escalate()

#     spawn(FalseAlarmParent)
#     assert exc_raised


# def test_optional_process_high_water_mark_emits_an_event_for_every_multiple_of_that_nr_of_msgs_in_the_queue():
#     spawn = DummyNode().spawn

#     class MyProc(Process):
#         hwm = 100  # emit warning every 100 pending messages in the queue

#         def run(self):
#             yield self.get()  # put in receive mode
#             yield Deferred()  # queue all further messages

#     p = spawn(MyProc)
#     p << 'ignore'

#     for _ in range(3):
#         for _ in range(99):
#             p << 'dummy'
#         with assert_one_event(HighWaterMarkReached):
#             p << 'dummy'


# ##
# ## SUBPROCESS

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


# ##
# ##  MIGRATION

# def test_TODO_migrating_an_actor_to_another_host_suspends_serializes_and_deserializes_it_and_makes_it_look_as_if_it_had_been_deployed_remotely():
#     pass


# def test_TODO_migrating_an_actor_doesnt_break_existing_refs():
#     pass


# def test_TODO_migrating_an_actor_redirects_all_actorrefs_to_it():
#     pass


# def test_TODO_actor_is_allowed_to_fail_to_be_serialized():
#     pass


# ##
# ## TYPED ACTORREFS

# def test_TODO_typed_actorrefs():
#     pass


# ##
# ## HELPFUL & ASSISTIVE TRACEBACKS

# def test_TODO_correct_traceback_is_always_reported():
#     pass


# def test_TODO_assistive_traceback_with_send_and_spawn():
#     pass


# def test_TODO_assistive_traceback_with_recursive_send():
#     pass


# def test_TODO_assistive_traceback_with_async_interaction():
#     pass


# ##
# ## DISPATCHING

# def test_TODO_suspend_and_resume_doesnt_change_global_message_queue_ordering():
#     pass


# SUPPORT

class MockException(Exception):
    pass


def DummyNode():
    return Node(hub=HubWithNoRemoting())


wrap_globals(globals())
