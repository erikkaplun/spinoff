from __future__ import print_function

from twisted.internet.defer import Deferred, fail

from spinoff.util.pattern_matching import Matcher


class EventSequenceMismatch(AssertionError):
    pass


class EvSeq(list):
    def __init__(self):
        list.__init__(self)
        self.waiter = None

    def __call__(self, event):
        self.append(event)
        if self.waiter:
            match = False
            try:
                match = self.waiter[0] == self
            except EventSequenceMismatch:
                self.waiter[1].errback()
            else:
                if match:
                    seq = self[:]
                    self[:] = []
                    self.waiter[1].callback(seq)

    def await(self, pattern):
        if isinstance(pattern, basestring):
            pattern = EVENT(pattern)
        for i in range(len(self)):
            seq = self[:i + 1]
            try:
                match = pattern == seq
            except EventSequenceMismatch:
                return fail()
            else:
                if match:
                    for _ in range(i + 1):
                        self.pop(0)
                    return seq
        else:
            d = Deferred()
            self.waiter = (pattern, d)
            return d


class EVENT(Matcher):
    """Matches if the last (i.e. current) event emitted is the specified event"""

    def __init__(self, event):
        self.event = event

    def __eq__(self, seq):
        return seq[-1] == self.event


class NEXT(EVENT):
    """Like EVENT, but only matches if the specified event is emitted as the first event from the start of the await.

    If another event is emitted, the entire await will fail.

    """
    def __eq__(self, seq):
        if seq != [self.event]:
            raise EventSequenceMismatch("Expected the next event to be %r but was %r" % (self.event, seq[-1]))
        else:
            return True


def print_comparison(got, expected):
    # format the expected and actual list of events into columns

    if len(got) < len(expected):
        got.extend([None] * (len(expected) - len(got)))
    if len(expected) < len(got):
        expected.extend([None] * (len(got) - len(expected)))
    assert len(got) == len(expected)

    l = max(len(x) for x in expected if x)
    cols = [('EXPECTED:', 'GOT:')] + zip(expected, got)
    cols_adjusted = [((x or '').ljust(l, ' '), y) for x, y in cols]
    cols_str = '\n'.join(('%s   %s' % (x if x else '', y if y else '')).strip() for x, y in cols_adjusted)

    return cols_str
