from twisted.internet import reactor
from twisted.internet.task import Clock
from twisted.trial import unittest

from unnamedframework.actor import IActor, RoutingException, Actor, InterfaceException
from unnamedframework.util.testing import assert_raises, assert_not_raises, deferred_result

from unnamedframework.actor.device.relay import Relay


class HttpGatewayTest(unittest.TestCase):

    def setUp(self):
        self._create_relay()

    def _create_relay(self, use_clock=False, **kwargs):
        self.clock = Clock() if use_clock else None
        self.relay = Relay(reactor=self.clock if use_clock else reactor, **kwargs)
        self.mock = Actor()
        self.relay.connect(to=self.mock)

        self.relay.start()
        self.addCleanup(self.relay.stop)

    def test_interface(self):
        x = self.relay

        assert IActor.providedBy(x)

        with assert_raises(InterfaceException):
            x.send(message=(1, ('send', ('whatev', None))))
        with assert_raises(InterfaceException):
            x.send(message=('send', ('whatev', 1)))

        with assert_not_raises(RoutingException):
            x.send(message=(1, ('send', ('whatev', 1))))

        with assert_raises(InterfaceException):
            x.send(message=(1, ('init', (None, ))))
        with assert_not_raises(InterfaceException):
            x.send(message=(1, ('init', ('some-id', ))))

    def test_delivery(self):
        x = self.relay
        mock = self.mock

        x.send(message=('node-1', ('init', [1])))
        x.send(message=('node-2', ('send', ['msg-1', 1])))

        _, msg = deferred_result(mock.get())
        assert msg == 'msg-1'

        x.send(message=('node-2', ('send', ['msg-2', 3])))

        msg_d = mock.get()
        assert not msg_d.called

        x.send(message=('node-3', ('init', [3])))

        sender, msg = deferred_result(msg_d)
        assert msg == 'msg-2'

        x.send(message=('node-2', ('send', ['msg-3', 3])))
        sender, msg = deferred_result(mock.get())
        assert msg == 'msg-3'

        x.send(message=('node-3', ('uninit', [])))
        x.send(message=('node-2', ('send', ['whatev', 3])))
        msg_d = mock.get()
        assert not msg_d.called

        with assert_raises(RoutingException):
            x.send(message=(3, ('uninit', [])))

    def test_message_timeout(self):
        self._create_relay(use_clock=True, max_message_age=10)

        self.relay.send(message=('node-1', ('send', ['msg-1', 2])))
        self.clock.advance(11)
        self.relay.send(message=('node-2', ('init', [2])))
        msg_d = self.mock.get()
        assert not msg_d.called, "messages younger than max_message_age are delivered"

        self.relay.send(message=('node-1', ('send', ['msg-2', 3])))
        self.clock.advance(9)
        self.relay.send(message=('node-3', ('init', [3])))
        assert msg_d.called, "messages younger than max_message_age are delivered"


if __name__ == '__main__':
    unittest.main()
