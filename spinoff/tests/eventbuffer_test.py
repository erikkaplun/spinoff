from twisted.trial import unittest
from twisted.internet.task import Clock

from spinoff.util.async import EventBuffer
from spinoff.util.testing import MockFunction


class EventBufferTestCase(unittest.TestCase):

    def test_basic(self):
        MS = 123

        mock_fn = MockFunction()

        clock = Clock()
        x = EventBuffer(mock_fn, reactor=clock, milliseconds=MS)

        x.call()
        assert mock_fn.called, "EventBuffer should fire on the first call"

        mock_fn.reset()

        x.call()
        assert not mock_fn.called, "EventBuffer should not fire when no time has passed from the last call"

        clock.advance(MS / 2)
        assert not mock_fn.called, "EventBuffer should not fire when not enough time has passed from the last call"

        clock.advance(MS)

        x.call()
        assert mock_fn.called, "EventBuffer should fire when enough time has passed from the last call"

        mock_fn.reset()

        clock.advance(MS * 10)
        x.call()
        assert mock_fn.called, "EventBuffer should fire when more than enough time has passed from the last call"
