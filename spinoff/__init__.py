from twisted.python.failure import Failure


_old_cleanFailure = Failure.cleanFailure


def cleanFailure(self):
    tb = self.tb
    _old_cleanFailure(self)
    self.tb = tb


# TODO: only do this in DEBUG mode
Failure.cleanFailure = cleanFailure
