from twisted.trial import unittest
from twisted.internet.task import Clock

from unnamedframework.util.async import EventBuffer


class EventBufferTestCase(unittest.TestCase):

    def test_basic(self):
        MS = 123

        called = []

        def reset():
            called[:] = []

        def set():
            called.append(1)

        clock = Clock()
        x = EventBuffer(set, reactor=clock, milliseconds=MS)

        x.call()
        assert called, "EventBuffer should fire on the first call"

        reset()

        x.call()
        assert not called, "EventBuffer should not fire when no time has passed from the last call"

        clock.advance(MS / 2)
        assert not called, "EventBuffer should not fire when not enough time has passed from the last call"

        clock.advance(MS)

        x.call()
        assert called, "EventBuffer should fire when enough time has passed from the last call"

        reset()

        clock.advance(MS * 10)
        x.call()
        assert called, "EventBuffer should fire when more than enough time has passed from the last call"
