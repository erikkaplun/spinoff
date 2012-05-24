from twisted.internet.defer import succeed, fail
from twisted.python.failure import Failure
from twisted.trial import unittest

from spinoff.util.async import combine
from spinoff.util.testing import errback_called, callback_called


class CombineTestCase(unittest.TestCase):

    def test_basic(self):
        f = lambda msg=None: fail(Failure(Exception(msg)))
        d = combine([])
        assert d.called, "An empty combine should return an already fired deferred when no deferreds were passed to it"

        d = combine([succeed(None)])
        assert callback_called(d), "ombine should return an already fired deferred when a success deferred was passed to it"

        d = combine([f()])
        assert errback_called(d)

        d = combine([succeed(None), f()])
        assert errback_called(d)

        d = combine([f(), succeed(None)])
        assert errback_called(d)

        d = combine([succeed(None), succeed(None), succeed(None), succeed(None)])
        assert not errback_called(d)
        assert callback_called(d)
