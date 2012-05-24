from spinoff.actor.actor import Actor, InterfaceException
from spinoff.util.testing import deferred_result, assert_not_raises, assert_raises


def test_basic():
    c = Actor()
    mock = Actor()
    c.connect('default', ('default', mock))

    c.put(message='whatev')
    with assert_not_raises():
        deferred_result(mock.get())

    c.put(message='msg-1', routing_key=123)
    with assert_raises(InterfaceException):
        deferred_result(mock.get())

    c.put(message='msg-1', routing_key=123)
    with assert_not_raises(InterfaceException):
        routing_key, msg = deferred_result(mock.get_routed())
    assert msg == 'msg-1'
    assert routing_key == 123

    c.put(message='msg-2', routing_key=321)
    routing_key, msg = deferred_result(mock.get_routed())
    assert msg == 'msg-2'
    assert routing_key == 321

    c.put(message='whatev')
    with assert_raises(InterfaceException):
        deferred_result(mock.get_routed())
