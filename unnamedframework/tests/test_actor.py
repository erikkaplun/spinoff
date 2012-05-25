from twisted.internet.defer import QueueUnderflow

from unnamedframework.actor.actor import Actor
from unnamedframework.util.async import CancelledError
from unnamedframework.util.testing import deferred_result, assert_raises, assert_not_raises


def test_basic():
    c = Actor()
    mock = Actor()
    c.connect('default', ('default', mock))

    c.put(message='msg-1')
    assert deferred_result(mock.get()) == 'msg-1'


def test_cancel_get():
    c = Actor()
    c._inboxes['default']
    d = c.get()
    with assert_raises(QueueUnderflow):
        c.get()

    ###
    c = Actor()
    c._inboxes['default']
    d = c.get()
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()
    with assert_not_raises(QueueUnderflow):
        c.get()
