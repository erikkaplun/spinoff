from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest
from txzmq import ZmqFactory

from spinoff.component.component import Component
from spinoff.component.transport.zeromq import ZmqRouter, ZmqDealer
from spinoff.util.async import TimeoutError, sleep, with_timeout
from spinoff.util.testing import assert_not_raises


_wait_msg = lambda d: with_timeout(0.2, d)
_wait_slow_joiners = lambda: sleep(0.0)


ADDR = 'ipc://test'


class RouterDealerTestCase(unittest.TestCase):

    def setUp(self):
        self._z_components = []
        self._z_factory = ZmqFactory()

    def _make(self, cls, endpoint, identity=None, with_mock=False):
        ret = cls(self._z_factory, endpoint, identity)
        self._z_components.append(ret)
        if with_mock:
            mocked_inboxes = with_mock
            assert isinstance(mocked_inboxes, (list, basestring, bool))

            if isinstance(mocked_inboxes, bool):
                mocked_inboxes = 'default'

            mock = Component()
            ret.connect(mocked_inboxes, mock)
            ret = ret, mock
        return ret

    def _make_dealer(self, *args, **kwargs):
        return self._make(ZmqDealer, *args, **kwargs)

    def _make_router(self, *args, **kwargs):
        return self._make(ZmqRouter, *args, **kwargs)

    @inlineCallbacks
    def test_router_with_one_dealer(self):
        router = self._make_router(ADDR)
        dealer, mock = self._make_dealer(ADDR, identity='dude', with_mock=True)
        yield _wait_slow_joiners()

        msg = 'PING'
        router.deliver(message=(dealer.identity, msg), inbox='default')

        with assert_not_raises(TimeoutError, "should have received a message"):
            assert msg == (yield _wait_msg(mock.get('default')))

    @inlineCallbacks
    def test_router_with_two_dealers(self):
        router = self._make_router(ADDR)
        dealer1, mock1 = self._make_dealer(ADDR, identity='dude1', with_mock=True)
        dealer2, mock2 = self._make_dealer(ADDR, identity='dude2', with_mock=True)
        yield _wait_slow_joiners()

        for i in [1, 2]:
            dealer = locals()['dealer%s' % i]
            mock = locals()['mock%s' % i]
            msg = 'PING%s' % i

            router.deliver(message=(dealer.identity, msg), inbox='default')
            with assert_not_raises(TimeoutError, "should have received a message"):
                assert msg == (yield _wait_msg(mock.get('default')))

    def tearDown(self):
        for component in self._z_components:
            component.stop()
