from unnamedframework.actor.actor import Actor
from unnamedframework.util.testing import deferred_result


def test_basic():
    c = Actor()
    mock = Actor()
    c.connect('default', ('default', mock))

    c.put(message='msg-1')
    assert deferred_result(mock.get()) == 'msg-1'
