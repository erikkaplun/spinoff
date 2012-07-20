from spinoff.util.python import enumrange
import sys
import random

from spinoff.util.python import EnumValue
from spinoff.util.testing import assert_raises
from spinoff.util.python import enums


def _make_random_name():
    return 'SOME_RANDOM_NAME_%s' % random.randint(0, sys.maxint)


def test_enum_value_name():
    random_name = _make_random_name()
    v1 = EnumValue(random_name)

    assert str(v1) == random_name
    assert repr(v1) == '<EnumValue %s>' % (random_name)

    v2 = EnumValue(random_name, order=123)
    assert str(v2) == random_name
    assert repr(v2) == '<EnumValue %s:%s>' % (random_name, 123)


def test_unordered_enum_value():
    v1 = EnumValue('V1')
    v2 = EnumValue('V2')

    assert v1 != v2, "two enum values should never be equal"
    assert v1 == v1, "enum value should always be equal to itself"
    assert v1 is v1, "enum value should always be identical to itself"

    with assert_raises(TypeError):
        v1 < v2

    with assert_raises(TypeError):
        v1 > v2

    with assert_raises(TypeError):
        EnumValue('X') < EnumValue('Y', order=1)
    with assert_raises(TypeError):
        EnumValue('X', order=1) < EnumValue('Y')
    with assert_raises(TypeError):
        EnumValue('X') > EnumValue('Y', order=1)
    with assert_raises(TypeError):
        EnumValue('X', order=1) > EnumValue('Y')


def test_ordered_enum_value():
    # the 2nd variant tests against default Python object ordering
    for o1, o2 in [(1, 2), (2, 1)]:
        v1 = EnumValue('V1', order=o1)
        v2 = EnumValue('V2', order=o2)

        assert v1 != v2, "two enum values should never be equal"
        assert v1 == v1, "enum value should always be equal to itself"
        assert v1 is v1, "enum value should always be identical to itself"

        assert v1 < v2 if o1 < o2 else v2 < v1, "enum value with order %s is less than enum value with order %s" % (o1, o2) if o1 < o2 else (o2, o1)
        assert v2 > v1 if o2 > o1 else v1 > v2, "enum value with order %s is greater than enum value with order %s" % (o2, o1) if o2 > o1 else (o1, o2)


def test_enum_values_with_same_order():
    v1, v2 = EnumValue('V1', 1), EnumValue('V2', 1)
    assert not (v1 < v2)
    assert not (v1 > v2)


def test_enums_with_no_order():
    names = [_make_random_name() for _ in range(3)]
    v1, v2, v3 = enums(*names)
    assert all(isinstance(x, EnumValue) for x in [v1, v2, v3])
    assert [str(v1), str(v2), str(v3)] == names
    assert v1 != v2 != v3 and v1 != v3


def test_enums_with_explicit_order():
    names = [_make_random_name() for _ in range(3)]
    v1, v2, v3 = enums(*[(order, name) for order, name in enumerate(names)])
    assert v1 < v2 < v3
    assert v1 != v2 != v3 and v1 != v3
    assert v3 > v2 > v1


def test_enums_with_mixed_ordered_and_unordered_items_is_not_allowed():
    with assert_raises(TypeError):
        enums('a', 'b', (1, 'c'), (2, 'd'))


def test_enum_value_names_must_be_unique():
    with assert_raises(TypeError):
        enums('a', 'a')
    with assert_raises(TypeError):
        enums('a', 'a', 'b')


def test_enumrange_aka_enums_with_implicit_order():
    names = [_make_random_name() for _ in range(3)]
    assert [(x.order, x.name) for x in enumrange(*names)] == [(order, name) for order, name in enumerate(names)]
