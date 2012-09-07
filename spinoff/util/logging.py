# coding: utf8
from __future__ import print_function

import inspect
import re
import sys
import traceback
import types
from collections import defaultdict


BLUE = '\x1b[1;34m'
CYAN = '\x1b[1;36m'
GREEN = '\x1b[1;32m'
RED = '\x1b[1;31m'

DARK_RED = '\x1b[0;31m'

RESET_COLOR = '\x1b[0m'

YELLOW = '\x1b[1;33m'

BLINK = '\x1b[5;31m'


OUTFILE = sys.stderr
LEVEL = 0

ENABLE_ONLY = False


LEVELS = [
    ('debug', GREEN),
    ('log', GREEN),
    ('log', GREEN),
    ('log', GREEN),
    ('log', GREEN),
    ('fail', YELLOW),
    ('flaw', YELLOW),
    ('error', RED),
    ('error', RED),
    ('panic', BLINK + RED),
    ('fatal', BLINK + RED),
]
LEVELS = [(name.ljust(5), style) for name, style in LEVELS]


def dbg(*args, **kwargs):
    _write(0, *args, **kwargs)


def dbg1(*args, **kwargs):
    _write(0, end='', *args, **kwargs)


# def dbg2(*args, **kwargs):
#     _write(0, end='.', *args, **kwargs)


def dbg3(*args, **kwargs):
    _write(0, end='\n', *args, **kwargs)


def log(*args, **kwargs):
    _write(1, *args, **kwargs)


def fail(*args, **kwargs):
    _write(5, *args, **kwargs)


def flaw(*args, **kwargs):
    """Logs a failure that is more important to the developer than a regular failure because there might be a static
    programming flaw in the code as opposed to a state/conflict/interaction induced one.

    """
    _write(6, *args, **kwargs)


def err(*args, **kwargs):
    _write(7, *((RED,) + args + (RESET_COLOR,)), **kwargs)


def panic(*args, **kwargs):
    _write(9, *((RED,) + args + (RESET_COLOR,)), **kwargs)


def fatal(*args, **kwargs):
    _write(10, *((RED,) + args + (RESET_COLOR,)), **kwargs)

_pending_end = defaultdict(bool)


_logstrings = {}


def _write(level, *args, **kwargs):
    try:
        if level >= LEVEL:
            frame = sys._getframe(2)
            caller = frame.f_locals.get('self', frame.f_locals.get('cls', None))

            f_code = frame.f_code
            file, lineno, caller_name = f_code.co_filename, frame.f_lineno, f_code.co_name
            file = file.rsplit('/', 1)[-1]

            if caller:
                caller_module = caller.__module__
                cls_name = caller.__name__ if isinstance(caller, type) else type(caller).__name__
                caller_full_path = '%s.%s' % (caller_module, cls_name)
            else:
                # TODO: find a faster way to get the module than inspect.getmodule
                caller = inspect.getmodule(frame)
                caller_full_path = caller_module = caller.__name__

            if ENABLE_ONLY and not any(re.match(x, caller_full_path) for x in ENABLE_ONLY):
                return

            caller_fn = getattr(caller, caller_name, None)

            logstring = getattr(caller_fn, '_r_logstring', None) if caller_fn else None
            if not logstring:
                logstring = getattr(caller_fn, '_logstring', None)
                if logstring:
                    if isinstance(logstring, unicode):
                        logstring = logstring.encode('utf8')
                else:
                    logstring = caller_name + ':'

                logstring = YELLOW + logstring + RESET_COLOR

                # cache it
                if isinstance(caller_fn, types.MethodType):
                    caller_fn.im_func._r_logstring = logstring
                elif caller_fn:
                    caller_fn._r_logstring = logstring

            logname = getattr(caller, '_r_logname', None)
            if not logname:
                logname = caller._r_logname = CYAN + get_logname(caller) + RESET_COLOR

            statestr = GREEN + ' '.join(k for k, v in get_logstate(caller).items() if v) + RESET_COLOR

            comment = get_logcomment(caller)

            loc = "%s:%s" % (file, lineno)
            if level >= 9:  # blink for panics
                loc = BLINK + loc + RESET_COLOR

            levelname = LEVELS[level][1] + LEVELS[level][0] + RESET_COLOR

            # args = tuple(x.encode('utf-8') for x in args if isinstance(x, unicode))
            print("%s  %s %s  %s  %s" %
                  (levelname, loc, logname, statestr, logstring),
                  file=OUTFILE, *(args + (comment,)), **kwargs)
    except Exception:
        # from nose.tools import set_trace; set_trace()
        print(RED, u"!!%d: (logger failure)" % (level,), file=sys.stderr, *args, **kwargs)
        print(traceback.format_exc(), RESET_COLOR, file=sys.stderr)


def get_logname(obj):
    return (repr(obj).strip('<>')
            if not isinstance(obj, type) else
            obj.__name__
            if not isinstance(obj, types.ModuleType) else
            '(module) ' + obj.__name__)


def get_logstate(obj):
    try:
        return obj.logstate()
    except AttributeError:
        return {}


def get_logcomment(obj):
    try:
        return '     ' + obj.logcomment()
    except AttributeError:
        return ''


def logstring(logstr):
    def dec(fn):
        fn._logstring = logstr
        return fn
    return dec
