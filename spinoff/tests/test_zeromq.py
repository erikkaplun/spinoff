from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest
from txzmq import ZmqFactory

from spinoff.component.component import Component
from spinoff.component.transport.zeromq import ZmqRouter, ZmqDealer
from spinoff.util.async import TimeoutError, sleep, with_timeout
from spinoff.util.testing import assert_not_raises


_wait_for_msg = lambda d: with_timeout(0.2, d)
_wait_slow_joiners = lambda: sleep(0.0)


ADDR = 'ipc://test'


class Mock(Component):
    pass


class TestCase1(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        self.mock = Mock()
        f = ZmqFactory()
        self.z_router = ZmqRouter(f, ('bind', ADDR))
        self.z_dealer = ZmqDealer(f, ('connect', ADDR), identity='dude')
        self.z_dealer.connect('default', self.mock)
        yield _wait_slow_joiners()

    @inlineCallbacks
    def test_router(self):
        self.z_router.deliver(message=('dude', 'PING'), inbox='default')

        with assert_not_raises(TimeoutError, "should have received a message"):
            msg = yield _wait_for_msg(self.mock.get('default'))
        assert msg == 'PING'

    def tearDown(self):
        self.z_dealer.stop()
        self.z_router.stop()
