from spinoff.actor.supervision import Restart, _Restart, Resume, Stop, Escalate, Default, Decision
from spinoff.util.pattern_matching import ANY


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
