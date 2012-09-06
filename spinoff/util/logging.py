# coding: utf8
from __future__ import print_function

import sys
import traceback
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
    _write(5, *((RED,) + args + (RESET_COLOR,)), **kwargs)


def err(*args, **kwargs):
    _write(7, *((RED,) + args + (RESET_COLOR,)), **kwargs)


def panic(*args, **kwargs):
    _write(9, *((RED,) + args + (RESET_COLOR,)), **kwargs)

_pending_end = defaultdict(bool)


_logstrings = {}


def _write(level, *args, **kwargs):
    try:
        if level >= LEVEL:
            frame = sys._getframe(2)
            caller = frame.f_locals['self']

            if ENABLE_ONLY and not any(caller.__module__.startswith(x) for x in ENABLE_ONLY):
                return

            f_code = frame.f_code
            file, lineno, caller_name = f_code.co_filename, f_code.co_firstlineno, f_code.co_name
            file = file.rsplit('/', 1)[-1]

            caller_fn = getattr(caller, caller_name)

            logstring = getattr(caller_fn, '_r_logstring', None)
            if not logstring:
                logstring = getattr(caller_fn, '_logstring', None)
                if logstring:
                    if isinstance(logstring, unicode):
                        logstring = logstring.encode('utf8')
                else:
                    logstring = caller_name.replace('_', '-') + ':'

                logstring = YELLOW + logstring.upper() + RESET_COLOR

                # cache it
                caller_fn.im_func._r_logstring = logstring

            logname = getattr(caller, '_r_logname', None)
            if not logname:
                logname = CYAN + get_logname(caller) + RESET_COLOR
                caller._r_logname = logname

            statestr = GREEN + ' '.join(k for k, v in get_logstate(caller).items() if v) + RESET_COLOR

            comment = get_logcomment(caller)

            loc = "%s:%s" % (file, lineno)
            if level >= 9:  # blink for panics
                loc = BLINK + loc + RESET_COLOR

            # args = tuple(x.encode('utf-8') for x in args if isinstance(x, unicode))
            print("%s %s  %s  %s" %
                  (loc, logname, statestr, logstring),
                  file=OUTFILE, *(args + (comment,)), **kwargs)
    except Exception:
        # from nose.tools import set_trace; set_trace()
        print(RED, u"!!%d: (logger failure)" % (level,), file=sys.stderr, *args, **kwargs)
        print(traceback.format_exc(), RESET_COLOR, file=sys.stderr)


def get_logname(obj):
    return repr(obj).strip('<>')


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
