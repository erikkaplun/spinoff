from spinoff.component.component import _normalize_pipe


def test_normalize_pipe():
    o = object()
    assert _normalize_pipe(o) == ('default', o, 'default')
    assert _normalize_pipe((o, )) == ('default', o, 'default')

    assert _normalize_pipe((o, 'default')) == ('default', o, 'default')
    assert _normalize_pipe(('default', o, 'default')) == ('default', o, 'default')

    assert _normalize_pipe((o, 'not-default')) == ('default', o, 'not-default')
    assert _normalize_pipe(('not-default', o)) == ('not-default', o, 'default')
    assert _normalize_pipe(('not-default', o, 'not-default')) == ('not-default', o, 'not-default')
