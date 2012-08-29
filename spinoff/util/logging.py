# coding: utf8
from __future__ import print_function

import inspect
import os
import sys


BLUE = '\x1b[1;34m'
CYAN = '\x1b[1;36m'
GREEN = '\x1b[1;32m'
RED = '\x1b[1;31m'

DARK_RED = '\x1b[0;31m'

RESET_COLOR = '\x1b[0m'


class Logging(object):
    OUTFILE = sys.stderr
    LEVEL = 0

    _ljust1 = 20

    def dbg(self, *args, **kwargs):
        self._write(0, *args, **kwargs)

    def dbg1(self, *args, **kwargs):
        self._write(0, end='', *args, **kwargs)

    def dbg2(self, *args, **kwargs):
        self._write(0, end='.', *args, **kwargs)

    def dbg3(self, *args, **kwargs):
        self._write(0, end='\n', *args, **kwargs)

    def log(self, *args, **kwargs):
        self._write(1, *args, **kwargs)

    def err(self, *args, **kwargs):
        self._write(5, *args, **kwargs)

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
                    caller_name += ':'

                logstr = self.logstring()

                statestr = ' '.join(k for k, v in self.logstate().items() if v)

                logcomment = ''
                if kwargs.get('end') != '':
                    logcomment = self._get_logcomment()

                print("%s:%s %s  %s  %s" % (file, lineno, logstr, statestr, caller_name.upper().replace('_', '-')),
                      file=self.OUTFILE, *(args + (logcomment,)), **kwargs)
        except Exception:
            print("!!%d: (logger failure)" % (level,), file=self.OUTFILE, *args, **kwargs)

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
