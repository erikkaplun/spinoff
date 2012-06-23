from unnamedframework.util.pattern_matching import match, ANY, IGNORE
from unnamedframework.util.testing import assert_not_raises
from unnamedframework.util.pattern_matching import NOTHING


def NO(pattern, data):
    x = match(pattern, data)
    assert isinstance(x, tuple) and not x[0] or not x, x


def YES(out, pattern, data):
    x = match(pattern, data)
    if out is not NOTHING:
        assert x[0], "should have matched"
        assert x[1] == out, "should have returned the correct values but returned %s" % repr(x[1])
    else:
        assert x


def test():
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
    YES(NOTHING, (IGNORE,), ('whatev',))
    YES(('foo',), (ANY, IGNORE), ('foo', 'whatev',))
    YES(NOTHING, (IGNORE, IGNORE), ('whatev', 'whatev',))
    YES(('foo', 'bar'), (ANY, ANY), ('foo', 'bar',))

    YES(NOTHING, ('foo', IGNORE), ('foo', 'whatev',))
    NO(('foo', IGNORE), ('WRONG', 'whatev',))
    NO(('foo', ANY), ('WRONG', 'whatev',))

    YES(NOTHING, ('foo', (IGNORE, )), ('foo', ('whatev', )))
    YES(((1, (2, (3,))), ), ('foo', (ANY, (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES((((2, (3,),),),), ('foo', (IGNORE, (ANY, (ANY, )))), ('foo', (1, (2, (3,)))))
    YES(((((3,),),),), ('foo', (IGNORE, (IGNORE, (ANY, )))), ('foo', (1, (2, (3,)))))

    with assert_not_raises(ValueError):
        _, ((_, (_, (_,))),) = match(
            ('foo', (ANY, (ANY, (ANY, )))),
            ('WRONG', (1, (2, (3,))))
            )
