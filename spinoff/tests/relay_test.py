from spinoff.util.testing import assert_no_warnings
from twisted.internet.task import Clock
from twisted.trial import unittest

from spinoff.actor.device.relay import Relay
from spinoff.util.testing import MockActor, Container, deref


class HttpGatewayTest(unittest.TestCase):

    def setUp(self):
        self._create_relay()

    def _create_relay(self, use_clock=False, **kwargs):
        self.clock = Clock()

        self.root = Container._spawn()
        self.mock = self.root.spawn(MockActor)
        self.relay = self.root.spawn(Relay(self.mock, reactor=self.clock, **kwargs))

        self.addCleanup(lambda: self.relay << 'stop')

    def test_delivery(self):
        x = self.relay
        mock = self.mock

        x.send(('node-1', ('init', [1])))
        x.send(('node-2', ('send', ['msg-1', 1])))
        assert deref(mock).clear()[-1] == ('node-1', 'msg-1')

        x.send(('node-2', ('send', ['msg-2', 3])))
        assert not deref(mock).messages

        x.send(('node-3', ('init', [3])))
        assert deref(mock).clear()[-1] == ('node-3', 'msg-2')

        x.send(('node-2', ('send', ['msg-3', 3])))
        assert deref(mock).clear()[-1] == ('node-3', 'msg-3')

        x.send(('node-3', ('uninit', [])))
        x.send(('node-2', ('send', ['whatev', 3])))
        assert not deref(mock).messages

    def test_message_timeout(self):
        self._create_relay(max_message_age=10)

        self.relay.send(('node-1', ('send', ['msg-1', 2])))
        self.clock.advance(11)
        self.relay.send(('node-2', ('init', [2])))
        assert not deref(self.mock).messages, "messages older than max_message_age are not delivered"

        self.relay.send(('node-1', ('send', ['msg-2', 3])))
        self.clock.advance(9)
        self.relay.send(('node-3', ('init', [3])))
        assert deref(self.mock).messages, "messages younger than max_message_age are delivered"


if __name__ == '__main__':
    unittest.main()
