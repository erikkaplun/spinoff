from spinoff.actor.events import Events, Terminated, Error


def test_basic():
    assert repr(Terminated('SomeActor@/foo/bar')) == "Terminated('SomeActor@/foo/bar')"
    assert repr(Terminated('SomeActor@/foo/bar')) == "Terminated('SomeActor@/foo/bar')"


def test_subscribe_and_unsubscribe():
    errors = []
    Events.subscribe(Error, errors.append)
    Events.log(Error('actor', 1, 2))
    assert errors == [Error('actor', 1, 2)]

    errors[:] = []
    Events.unsubscribe(Error, errors.append)
    event = Error('actor', 1, 2)
    Events.log(event)
    assert errors == []
