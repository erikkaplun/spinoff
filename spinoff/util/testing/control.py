from __future__ import print_function

import abc
from pickle import PicklingError

from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from spinoff.util.async import sleep, after, with_timeout


class Holder(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def value(self):
        pass

    @abc.abstractproperty
    def defaultvalue(self):
        pass

    def __eq__(self, value):
        return self.value == value

    def __req__(self, value):
        return self.__eq__(value)

    def __ne__(self, value):
        return not (self == value)

    def __le__(self, value):
        return self < value or self == value

    def __ge__(self, value):
        return self > value or self == value

    def reset(self):
        self.value = self.defaultvalue

    def __nonzero__(self):
        return self.value

    def __getstate__(self):
        raise PicklingError("Object of type %s is not picklable" % (type(self).__name__,))


class Latch(Holder):
    """A flag object that goes from `False` to `True` when invoked.

    >>> l = Latch()
    >>> bool(l)
    False
    >>> l()
    >>> bool(l)
    True
    >>> l.reset()
    >>> bool(l)
    False

    """
    defaultvalue = value = False

    def __call__(self):
        self.value = True


class Counter(Holder):
    """A counter object whose value is incremented each time it's invoked.

    >>> c = Counter()
    >>> c.value
    0
    >>> c == 0
    True
    >>> bool(c)
    False
    >>> c > 0
    False
    >>> c()
    >>> c == 1
    True
    >>> c.value
    1
    >>> bool(c)
    True
    >>> c > 0
    True
    >>> c < 2
    True
    >>> c >= 2
    False
    >>> c()
    >>> c.value
    2
    >>> c >= 2
    True
    >>> c()
    >>> c >= 2
    True
    >>> c.reset()
    >>> c.value
    0

    """
    value = defaultvalue = 0

    def __call__(self):
        self.value += 1

    def __lt__(self, value):
        return self.value < value

    def __gt__(self, value):
        return self.value > value

    def __repr__(self):
        return repr(self.value)


class EMPTY(object):
    __repr__ = lambda _: 'EMPTY'
    __nonzero__ = lambda _: False
EMPTY = EMPTY()


class Slot(Holder):
    """Stores the single argument when invoked.

    >>> s = Slot()
    >>> bool(s)
    False
    >>> s()
    EMPTY
    >>> s == None
    False
    >>> s == 123
    False
    >>> s << 123
    >>> s()
    123
    >>> s == 123
    True
    >>> bool(s)
    True
    >>> s.set(321)
    >>> s == 321
    True
    >>> s.reset()
    >>> bool(s)
    False

    """
    value = defaultvalue = EMPTY

    def __call__(self):
        return self.value

    def set(self, value):
        self.value = value
    __lshift__ = set

    def __nonzero__(self):
        return self.value is not EMPTY

    def __repr__(self):
        return repr(self.value)


class Trigger(Deferred):
    """Like a `Deferred` but can be fired by calling it.

    Just like with a Deferred, control is immediately passed over to the one that is blocking on the `Trigger`.

    >>> s = Slot()
    >>> t = Trigger().addCallback(s.set)
    >>> t.called
    0
    >>> t.threshold
    1
    >>> t()
    >>> bool(s)
    True

    >>> s = Slot()
    >>> t = Trigger().addCallback(s.set)
    >>> t(123)
    >>> s == 123
    True

    >>> s = Slot()
    >>> t = Trigger(threshold=2).addCallback(s.set)
    >>> t()
    >>> bool(s)
    False
    >>> t()
    >>> bool(s)
    True

    >>> t = Trigger(threshold=3)
    >>> t(123)
    Traceback (most recent call last):
    ...
    TypeError: Trigger with threshold called with a parameter

    """
    def __init__(self, threshold=1):
        Deferred.__init__(self)
        assert threshold >= 1, "threshold should be at least 1"
        self.threshold = threshold
        self.times_called = 0

    def __call__(self, param=None):
        if self.threshold > 1 and param is not None:
            raise TypeError("Trigger with threshold called with a parameter")

        self.times_called += 1
        if self.times_called == self.threshold:
            self.callback(param)


class Barrier(Trigger):
    """Otherwise identical to `Trigger` (and thus `Deferred`) except invoking it returns to the caller (mock actor)
    without passing control to the receiver of the signal (test case).

    This is useful when the receiver (test case) simply does not want to continue before a condition has been met at the
    sender's end, or situation reached in the sender,  but requires (or is simply invariant to) additional processing
    in the sender (mock actor) before execution continues in the receiver context (test case).

    This is similar to barrier synchronisation except it is deterministic (and, figuratively, unfair) in the sense that
    the last (second) flow to reach the barrier is the one that escapes it first.

    Example:

        message_received = Signal()

        class MockActor(Actor):
            def receive(self, _):
                message_received()
                print("receive continues after emitting signal")

        spawn(MockActor).send('whatever', force_async=True)

        yield message_received

    """
    def __init__(self, *args, **kwargs):
        Trigger.__init__(self, *args, **kwargs)
        # the final callback will be invoked "a bit" later, thus allowing the invoker of `.callback(...)` to proceed.
        self.addCallback(lambda _: sleep(0))


class Buffer(object):
    def __init__(self):
        self.queue = []
        self.d = None

    def __call__(self, arg=None):
        if self.d:
            d, self.d = self.d, None
            d.callback(arg)
        else:
            self.queue.append(arg)

    @inlineCallbacks
    def expect(self, atleast=None, exactly=None, timeout=1.0):
        """If atleast > 1, returns `None`."""
        assert bool(exactly is None) != bool(atleast is None), "exactly one of `atleast` or `exactly` is required"
        if atleast is None:
            atleast = exactly if exactly else 1
        ret = []
        for _ in range(atleast):
            if self.queue:
                ret.append(self.queue.pop(0))
            else:
                self.d = Deferred()
                ret.append((yield with_timeout(timeout, self.d)))
        if exactly:
            yield self.expect_none()
        returnValue(ret)

    @inlineCallbacks
    def expect_one(self, *args, **kwargs):
        returnValue((yield self.expect(atleast=1, *args, **kwargs))[0])

    def expect_none(self):
        """If the queue is not empty, returns False immediately, otherwise a Deferred that fires a bit later and whose
        result is True or False depending on whether the queue is still empty when the Deferred fires or not.

        The Deferred mechanism is needed to allow other coroutines to perform tasks which can potentially affect the
        state of the queue.

        """
        def check_queue(_=None):
            if self.queue:
                raise AssertionError("Negative expectation failed")

        check_queue()
        return after(0.002).do(check_queue)
