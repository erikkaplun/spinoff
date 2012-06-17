from spinoff.util.pattern import match, _


def test_match():
    assert (True, ()) == match((), ())
    assert (False, None) == match((1,), ())
    assert (False, None) == match((), (1,))
    assert (True, ()) == match((1, ), (1, ))
    assert (True, (1, )) == match((_, ), (1, ))
    assert (False, None) == match((_, _), (1, ))
    assert (True, (1, 2)) == match((_, _), (1, 2))
    assert (True, (1, 3)) == match((_, 2, _), (1, 2, 3))
    assert (False, None) == match((_, 2, _), (1, 4, 3))

    assert (False, None) == match((_, 2, _), None)
