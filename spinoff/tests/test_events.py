from spinoff.actor.events import (
    UnhandledError, Events, UnhandledMessage, Terminated, Started, Suspended, Resumed, MessageReceived)


def test_basic():
    assert repr(MessageReceived('SomeActor@/foo/bar', 'some-message')) == \
        "MessageReceived('SomeActor@/foo/bar', message='some-message')"
    assert repr(UnhandledMessage('SomeActor@/foo/bar', 'some-message')) == \
        "UnhandledMessage('SomeActor@/foo/bar', message='some-message')"

    assert repr(Started('SomeActor@/foo/bar')) == \
        "Started('SomeActor@/foo/bar')"

    assert repr(Suspended('SomeActor@/foo/bar')) == \
        "Suspended('SomeActor@/foo/bar')"
    assert repr(Suspended('SomeActor@/foo/bar', Exception('message'))) == \
        "Suspended('SomeActor@/foo/bar', reason=Exception('message',))"

    assert repr(Resumed('SomeActor@/foo/bar')) == \
        "Resumed('SomeActor@/foo/bar')"

    assert repr(Terminated('SomeActor@/foo/bar')) == \
        "Terminated('SomeActor@/foo/bar')"
    assert repr(Terminated('SomeActor@/foo/bar', Exception('message'))) == \
        "Terminated('SomeActor@/foo/bar', reason=Exception('message',))"


def test_equality():
    assert UnhandledMessage('SomeActor@/foo/bar', 'some-message') == \
        UnhandledMessage('SomeActor@/foo/bar', 'some-message')


def test_subscribe_and_unsubscribe():
    errors = []
    Events.subscribe(UnhandledError, errors.append)
    Events.log(UnhandledError('actor', 1, 2))
    assert errors == [UnhandledError('actor', 1, 2)]

    errors[:] = []
    Events.unsubscribe(UnhandledError, errors.append)
    event = UnhandledError('actor', 1, 2)
    Events.log(event)
    assert errors == []
