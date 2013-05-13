from __future__ import print_function

import functools
import gc
import inspect
import sys
from contextlib import contextmanager

from gevent import with_timeout, Timeout, sleep
from nose.tools import ok_, eq_

from spinoff.actor import Actor
from spinoff.actor.events import Events, Error

from .common import assert_raises


class MockActor(Actor):
    def __init__(self, messages):
        self.receive = messages.append


@contextmanager
def expect_failure(exc, message=None, timeout=None):
    with assert_raises(exc, message=message) as basket:
        err = Events.consume_one(Error)
        yield basket
        try:
            _, exc, tb = err.get(timeout=timeout)
        except Timeout:
            pass
        else:
            raise exc, None, tb


@contextmanager
def expect_one_event(ev, timeout=None):
    result = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    yield
    try:
        result = result.get(timeout=timeout)
    except Timeout:
        ok_(False,
            "Event %r should have been emitted but was not" % (ev,)
            if not isinstance(ev, type) else
            "Event of type %s should have been emitted but was not" % (ev.__name__,))
    if isinstance(ev, type):
        ok_(isinstance(result, ev), "Event of type %s.%s should have been emitted but was not" % (ev.__module__, ev.__name__))
    else:
        eq_(result, ev, "Event %r should have been emitted but %s was" % (ev, result))


@contextmanager
def expect_event_not_emitted(ev, during=0.001):
    result = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    yield
    sleep(during)
    ok_(not result.ready() or (not isinstance(result.get(), ev) if isinstance(ev, type) else result.get() != ev),
        "Event %s should not have been emitted" % (" of type %s" % (ev.__name__,) if isinstance(ev, type) else ev,))


def wrap_globals(globals):
    """Ensures that errors in actors during tests don't go unnoticed."""

    def wrap(fn):
        @functools.wraps(fn)
        def ret(*args, **kwargs):
            # dbg("\n============================================\n")

            # TODO: once the above TODO (fresh Node for each test fn) is complete, consider making Events non-global by
            # having each Node have its own Events instance.
            Events.reset()

            def check_memleaks():
                if '__pypy__' not in sys.builtin_module_names:
                    gc.collect()
                    assert not gc.garbage, "Memory leak detected: %r" % (gc.garbage,)

                    # if gc.garbage:
                    #     dbg("GARGABE: detected after %s:" % (fn.__name__,), len(gc.garbage))
                    #     import objgraph as ob
                    #     import os

                    #     def dump_chain(g_):
                    #         isframework = lambda x: type(x).__module__.startswith(spinoff.__name__)
                    #         ob.show_backrefs([g_], filename='backrefs.png', max_depth=100, highlight=isframework)

                    #     for gen in gc.garbage:
                    #         dump_chain(gen)
                    #         dbg("   TESTWRAP: mem-debuggin", gen)
                    #         import pdb; pdb.set_trace()
                    #         os.remove('backrefs.png')

            errors = []
            Events.subscribe(Error, errors.append)
            try:
                return with_timeout(fn.timeout if hasattr(fn, 'timeout') else 0.5, fn, *args, **kwargs)
            finally:
                Events.unsubscribe(Error, errors.append)
                if errors:
                    sender, exc, tb = errors[0]
                    raise exc, None, tb
                del errors[:]
                check_memleaks()
        return ret

    for name, value in globals.items():
        if name.startswith('test_') and callable(value) and not inspect.isgeneratorfunction(value):
            globals[name] = wrap(value)
