# coding: utf8
from __future__ import print_function

import inspect
import os
import sys
import traceback


BLUE = '\x1b[1;34m'
CYAN = '\x1b[1;36m'
GREEN = '\x1b[1;32m'
RED = '\x1b[1;31m'

DARK_RED = '\x1b[0;31m'

RESET_COLOR = '\x1b[0m'

YELLOW = '\x1b[1;33m'

BLINK = '\x1b[5;31m'


class Logging(object):
    OUTFILE = sys.stderr
    LEVEL = 0

    def dbg(self, *args, **kwargs):
        self._write(0, *args, **kwargs)

    def dbg1(self, *args, **kwargs):
        self._write(0, end='', *args, **kwargs)

    # def dbg2(self, *args, **kwargs):
    #     self._write(0, end='.', *args, **kwargs)

    def dbg3(self, *args, **kwargs):
        self._write(0, end='\n', *args, **kwargs)

    def log(self, *args, **kwargs):
        self._write(1, *args, **kwargs)

    def fail(self, *args, **kwargs):
        self._write(5, *((RED,) + args + (RESET_COLOR,)), **kwargs)

    def err(self, *args, **kwargs):
        self._write(7, *((RED,) + args + (RESET_COLOR,)), **kwargs)

    def panic(self, *args, **kwargs):
        self._write(9, *((RED,) + args + (RESET_COLOR,)), **kwargs)

    _pending_end = False

    def _write(self, level, *args, **kwargs):
        try:
            if level >= self.LEVEL:
                if kwargs.get('end') == '\n':
                    print(' ', file=self.OUTFILE, *(args + (self._get_logcomment(),)), **kwargs)
                    self._pending_end = False
                    return
                elif kwargs.get('end') == '':
                    self._pending_end = True

                info = inspect.stack()[2]
                file, lineno, caller_name = info[1].rsplit(os.path.sep, 1)[-1], info[2], info[3]

                caller_fn = getattr(self, caller_name)
                if hasattr(caller_fn, 'logstring'):
                    caller_name = caller_fn.logstring
                    if isinstance(caller_name, unicode):
                        caller_name = caller_name.encode('utf8')
                else:
                    caller_name = caller_name.replace('_', '-') + ':'
                caller_name = YELLOW + caller_name.upper() + RESET_COLOR

                logstr = CYAN + self.logstring() + RESET_COLOR

                statestr = GREEN + ' '.join(k for k, v in self.logstate().items() if v) + RESET_COLOR

                logcomment = ''
                if kwargs.get('end') != '':
                    logcomment = self._get_logcomment()

                loc = "%s:%s" % (file, lineno)
                if level >= 9:  # blink for panics
                    loc = BLINK + loc + RESET_COLOR

                print("%s %s  %s  %s" %
                      (loc, logstr, statestr, caller_name),
                      file=self.OUTFILE, *(args + (logcomment,)), **kwargs)
        except Exception:
            print("!!%d: (logger failure)" % (level,), file=sys.stderr, *args, **kwargs)
            traceback.print_exc()

    def logstring(self):
        return repr(self).strip('<>')

    def logstate(self):
        return {}

    def logcomment(self):
        return ''

    def _get_logcomment(self):
        logcomment = self.logcomment()
        if logcomment:
            return '     ' + logcomment
        else:
            return ''


def logstring(logstr):
    def dec(fn):
        fn.logstring = logstr
        return fn
    return dec
