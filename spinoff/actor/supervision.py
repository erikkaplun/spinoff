from spinoff.util.pattern_matching import ANY


_EMPTY = object()


class Decision(object):
    class __metaclass__(type):
        def __instancecheck__(self, other):
            return type.__instancecheck__(self, other) or other is None and self is Decision

    def __repr__(self):
        return type(self).__name__


class Resume(Decision):
    """The 'resume' supervision decision."""
Resume = Resume()


class _Restart(Decision):
    """The 'restart' supervision decision.

    `max` and `within` are only considered to be defined if they are positive integers.

    """
    max, within = None, None

    def __init__(self, max=None, within=None):
        if max:
            self.max = max
        if within:
            if not max:
                raise TypeError("Restart.within not applicable if Restart.max is not defined")
            self.within = within

    def __call__(self, *args, **kwargs):
        assert not self.__dict__, "Can't construct Restart instances from existing non-default instances"
        if not args and not kwargs:
            return self
        return type(self)(*args, **kwargs)

    def __repr__(self):
        return (
            'Restart' if not self.max else
            'Restart(max=%d)' % self.max if not self.within else
            'Restart(max=%d, within=%d)' % (self.max, self.within)
        )

    def __eq__(self, other):
        return isinstance(other, _Restart) and other.max == self.max and other.within == self.within

    __req__ = __eq__

    def __ne__(self, other):
        return not (self == other)
Restart = _Restart()


class Stop(Decision):
    pass
Stop = Stop()


class Escalate(Decision):
    pass
Escalate = Escalate()


class Default(Decision):
    pass
Default = Default()


def test():
    from spinoff.util.testing import assert_raises

    with assert_raises(AssertionError):
        Restart(1)()

    assert Restart == Restart()

    assert Restart is Restart()

    assert Restart == Restart(ANY, ANY)
    assert Restart(1) == Restart(1)
    assert Restart(1) == Restart(ANY)
    assert Restart(ANY) == Restart(1)
    assert Restart(1, 2) == Restart(1, 2)
    assert Restart(1, 2) == Restart(1, 2)

    assert Restart(1, 2) != Restart(3, 4)

    with assert_raises(TypeError):
        Restart(within=3)

    assert repr(Restart) == 'Restart'
    assert repr(Restart(1)) == 'Restart(max=1)'
    assert repr(Restart(1, 2)) == 'Restart(max=1, within=2)'

    assert Restart != 'foo'

    for decision in [Resume, Restart, Stop, Escalate, Default, None]:
        assert isinstance(decision, Decision), decision

    assert not isinstance(None, _Restart)
