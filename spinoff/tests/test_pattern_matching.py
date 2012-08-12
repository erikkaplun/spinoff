from spinoff.util.testing import assert_not_raises
from spinoff.util.pattern_matching import match, ANY, IGNORE, IS_INSTANCE, NOT


FLATTEN = True


def NO(outlen, pattern, data):
    x = match(pattern, data, flatten=FLATTEN)
    assert not x[0] if isinstance(x, tuple) else not x, x

    if FLATTEN:
        assert isinstance(x, bool) if outlen == 0 else len(x[1:]) == outlen
    else:
        assert type(x[1]) is tuple
        assert len(x[1]) == outlen, "length should have been %s but was %s" % (outlen, len(x[1]))


def YES(out, pattern, data):
    x = match(pattern, data, flatten=FLATTEN)
    if not FLATTEN:
        assert x[0], "should have matched"
        assert type(x[1]) is tuple
        assert x[1] == out, "should have returned %s but returned %s" % (repr(out), repr(x[1]))
    else:
        assert x is True if out == () else x[0], "should have matched"
        assert out == () or x[1:] == out, "should have returned %s but returned %s" % (repr(out), repr(x[1:]))


def test_without_flatten():
    global FLATTEN
    FLATTEN = False

    NO(0, 'foo', 'bar')
    YES((), 'foo', 'foo')
    NO(0, (), 'whatev')
    YES((), (), ())

    YES(('whatev', ), ANY, 'whatev')
    YES((('whatev', 'whatev'), ), ANY, ('whatev', 'whatev'))

    YES((), ('foo',), ('foo',))
    NO(0, ('whatev',), ('whatev', 'whatev'))
    NO(0, ('whatev', 'whatev'), ('whatev',))

    YES((('foo',), ), ANY, ('foo',))
    YES(('foo',), (ANY,), ('foo',))
    YES((), (IGNORE(ANY),), ('whatev',))
    YES(('foo',), (ANY, IGNORE(ANY)), ('foo', 'whatev',))
    YES((), (IGNORE(ANY), IGNORE(ANY)), ('whatev', 'whatev',))
    YES(('foo', 'bar'), (ANY, ANY), ('foo', 'bar',))

    YES((), ('foo', IGNORE(ANY)), ('foo', 'whatev',))
    NO(0, ('foo', IGNORE(ANY)), ('WRONG', 'whatev',))
    NO(1, ('foo', ANY), ('WRONG', 'whatev',))

    YES((), ('foo', (IGNORE(ANY), )), ('foo', ('whatev', )))
    YES((1, 2, 3), ('foo', (ANY, (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((2, 3), ('foo', (IGNORE(ANY), (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((3, ), ('foo', (IGNORE(ANY), (IGNORE(ANY), (ANY, )))), ('foo', (1, (2, (3,)))))

    with assert_not_raises(ValueError):
        _, (_, _, _) = match(
            ('foo', (ANY, (ANY, (ANY, )))),
            ('WRONG', (1, (2, (3,)))),
            flatten=False)

    with assert_not_raises(ValueError):
        _, _, _, _ = match(
            ('foo', (ANY, (ANY, (ANY, )))),
            ('WRONG', (1, (2, (3,)))),
            flatten=True)


def test_flatten():
    global FLATTEN
    FLATTEN = True

    NO(0, 'foo', 'bar')

    YES((), 'foo', 'foo')
    YES((), (), ())

    YES(('whatev',), ANY, 'whatev')
    YES((('whatev', 'whatev'), ), ANY, ('whatev', 'whatev'))

    YES((), ('foo',), ('foo',))

    YES((('foo',),), ANY, ('foo',))
    YES(('foo',), (ANY,), ('foo',))
    YES((), (IGNORE(ANY),), ('whatev',))
    YES(('foo',), (ANY, IGNORE(ANY)), ('foo', 'whatev',))
    YES((), (IGNORE(ANY), IGNORE(ANY)), ('whatev', 'whatev',))
    YES(('foo', 'bar'), (ANY, ANY), ('foo', 'bar',))

    YES((), ('foo', IGNORE(ANY)), ('foo', 'whatev',))

    YES((), ('foo', (IGNORE(ANY), )), ('foo', ('whatev', )))
    YES((1, 2, 3,), ('foo', (ANY, (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((2, 3,), ('foo', (IGNORE(ANY), (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((3,), ('foo', (IGNORE(ANY), (IGNORE(ANY), (ANY, )))), ('foo', (1, (2, (3,)))))

    ok, v1, v2 = match(('foo', ('bar', ANY, ('baz', ANY))),
                       ('foo', ('bar', 123, ('baz', 456))),
                       flatten=True)
    assert ok and (v1, v2) == (123, 456)


def test_advanced():
    global FLATTEN
    FLATTEN = True

    assert IS_INSTANCE(int) == 1
    YES((1,), IS_INSTANCE(int), 1)
    YES((), IGNORE(IS_INSTANCE(int)), 1)


def test_not():
    assert NOT(IS_INSTANCE(int)) == 'string'
    for val in ['string', 123, True, None, object(), 123.456]:
        assert NOT(ANY) != val
    assert NOT(NOT(IS_INSTANCE(int))) == 3


def test_or():
    assert (IS_INSTANCE(int) | IS_INSTANCE(float)) == 3
    assert (IS_INSTANCE(int) | IS_INSTANCE(float)) == 3.3
    assert not ((IS_INSTANCE(int) | IS_INSTANCE(float)) == 'hello')
