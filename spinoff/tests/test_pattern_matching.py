from spinoff.util.pattern_matching import match, ANY, IGNORE
from spinoff.util.testing import assert_not_raises
from spinoff.util.pattern_matching import NOTHING
from spinoff.util.pattern_matching import IS_INSTANCE


FLATTEN = True


def NO(pattern, data):
    x = match(pattern, data)
    assert isinstance(x, tuple) and not x[0] or not x, x


def YES(out, pattern, data):
    x = match(pattern, data, flatten=FLATTEN)
    if not FLATTEN:
        if out is not NOTHING:
            assert x[0], "should have matched"
            assert x[1] == out, "should have returned the correct values but returned %s" % repr(x[1])
        else:
            assert x
    else:
        assert x if out == () else x[0], "should have matched"
        assert out == () or x[1:] == out, "should have returned the correct values but returned %s" % repr(x[1:])


def test_without_flatten():
    global FLATTEN
    FLATTEN = False

    NO('foo', 'bar')
    YES(NOTHING, 'foo', 'foo')
    NO((), 'whatev')
    YES(NOTHING, (), ())

    YES('whatev', ANY, 'whatev')
    YES(('whatev', 'whatev'), ANY, ('whatev', 'whatev'))

    YES(NOTHING, ('foo',), ('foo',))
    NO(('whatev',), ('whatev', 'whatev'))
    NO(('whatev', 'whatev'), ('whatev',))

    YES(('foo',), ANY, ('foo',))
    YES(('foo',), (ANY,), ('foo',))
    YES(NOTHING, (IGNORE(ANY),), ('whatev',))
    YES(('foo',), (ANY, IGNORE(ANY)), ('foo', 'whatev',))
    YES(NOTHING, (IGNORE(ANY), IGNORE(ANY)), ('whatev', 'whatev',))
    YES(('foo', 'bar'), (ANY, ANY), ('foo', 'bar',))

    YES(NOTHING, ('foo', IGNORE(ANY)), ('foo', 'whatev',))
    NO(('foo', IGNORE), ('WRONG', 'whatev',))
    NO(('foo', ANY), ('WRONG', 'whatev',))

    YES(NOTHING, ('foo', (IGNORE(ANY), )), ('foo', ('whatev', )))
    YES(((1, (2, (3,))), ), ('foo', (ANY, (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((((2, (3,),),),), ('foo', (IGNORE(ANY), (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES(((((3,),),),), ('foo', (IGNORE(ANY), (IGNORE(ANY), (ANY, )))), ('foo', (1, (2, (3,)))))

    with assert_not_raises(ValueError):
        _, ((_, (_, (_,))),) = match(
            ('foo', (ANY, (ANY, (ANY, )))),
            ('WRONG', (1, (2, (3,)))),
            flatten=False
            )


def test_flatten():
    global FLATTEN
    FLATTEN = True

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
