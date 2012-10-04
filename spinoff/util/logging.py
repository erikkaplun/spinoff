# coding: utf8
from __future__ import print_function, absolute_import

import inspect
import re
import sys
import traceback
import types
import os
import multiprocessing
from collections import defaultdict
from Queue import Queue, Empty

try:
    import colorama
except ImportError:
    colorama = None


WIN32 = sys.platform == 'win32'


_lock = multiprocessing.Lock()
_queue = Queue()


if WIN32:
    import msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)

    from encodings.aliases import aliases
    aliases['cp65001'] = 'utf_8'

    class UniStream(object):
        __slots__ = 'fileno', 'softspace',

        def __init__(self, fileobject):
            self.fileno = fileobject.fileno()
            self.softspace = False

        def write(self, text):
            os.write(self.fileno, text.encode('utf_8') if isinstance(text, unicode) else text)

        def flush(self):
            pass  # XXX: ?

        def isatty(self):
            return True  # XXX: hack so that colorama would wrap us

    sys.stdout = UniStream(sys.stdout)
    sys.stderr = UniStream(sys.stderr)

    if colorama:
        colorama.init()
    else:
        print("Colored log output disabled on WIN32; easy_install colorama to enable")


if not WIN32 or colorama:
    BLUE = '\x1b[1;34m'
    CYAN = '\x1b[1;36m'
    GREEN = '\x1b[1;32m'
    RED = '\x1b[1;31m'

    DARK_RED = '\x1b[0;31m'

    RESET_COLOR = '\x1b[0m'

    YELLOW = '\x1b[1;33m'

    BLINK = '\x1b[5;31m'
else:
    BLUE = ''
    CYAN = ''
    GREEN = ''
    RED = ''

    DARK_RED = ''

    RESET_COLOR = ''

    YELLOW = ''

    BLINK = ''

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


def get_calling_context(frame):
    caller = frame.f_locals.get('self', frame.f_locals.get('cls', None))

    f_code = frame.f_code
    file, lineno, caller_name = f_code.co_filename, frame.f_lineno, f_code.co_name
    file = file.rsplit('/', 1)[-1]

    return file, lineno, caller_name, caller


def _write(level, *args, **kwargs):
    _queue.put_nowait((level, args, kwargs))

    if _lock.acquire(False):
        try:
            while True:
                try:
                    pending = _queue.get_nowait()
                except Empty:
                    break
                else:
                    _do_write(pending[0], *pending[1], **pending[2])
        finally:
            _lock.release()


def _do_write(level, *args, **kwargs):
    try:
        if level >= LEVEL:

            frame = sys._getframe(3)
            file, lineno, caller_name, caller = get_calling_context(frame)

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
                # TODO: add logstring "inheritance"
                logstring = getattr(caller_fn, '_logstring', None)
                if logstring:
                    if isinstance(logstring, unicode):
                        logstring = logstring.encode('utf8')
                else:
                    logstring = caller_name + (':' if args else '')

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

            file = os.path.split(file)[-1]
            loc = "%s:%s" % (file, lineno)
            if level >= 9:  # blink for panics
                loc = BLINK + loc + RESET_COLOR

            levelname = LEVELS[level][1] + LEVELS[level][0] + RESET_COLOR

            dump_parent_caller = kwargs.pop('caller', False)
            # args = tuple(x.encode('utf-8') for x in args if isinstance(x, unicode))
            print(("%s %s  %s %s  %s  in %s" %
                  (os.getpid(), levelname, loc, logname, statestr, logstring)),
                  file=OUTFILE, *(args + (comment,)))
            if dump_parent_caller:
                parent_frame = frame
                for i in range(dump_parent_caller):
                    parent_frame = parent_frame.f_back
                    file_, lineno, caller_name, caller = get_calling_context(parent_frame)
                    loc = "%s:%s" % (file_, lineno)
                    print(" " * (i + 1) + "(invoked by) %s  %s  %s" % (get_logname(caller), caller_name, loc), file=OUTFILE)
    except Exception:
        # from nose.tools import set_trace; set_trace()
        print(RED, "!!%d: (logger failure)" % (level,), file=sys.stderr, *args, **kwargs)
        print(RED, "...while trying to log", repr(args), repr(comment) if 'comment' in locals() else '')
        print(traceback.format_exc(), RESET_COLOR, file=sys.stderr)


def get_logname(obj):
    return (obj.__name__
            if isinstance(obj, type) else
            repr(obj).strip('<>')
            if not isinstance(obj, types.ModuleType) else
            'module ' + obj.__name__)


def get_logstate(obj):
    try:
        return obj.logstate()
    except AttributeError:
        return {}


def get_logcomment(obj):
    try:
        x = obj.logcomment
    except AttributeError:
        return ''
    else:
        return '     ' + x()


def logstring(logstr):
    def dec(fn):
        fn._logstring = logstr
        return fn
    return dec
