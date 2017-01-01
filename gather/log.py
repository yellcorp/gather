import functools
import sys


__all__ = (
    "Logger",
    "CRITICAL", "ERROR", "WARNING", "INFO", "VERBOSE", "DEBUG",
)


CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
VERBOSE = 15
DEBUG = 10


def log_method(level):
    def method(self, template, *targs, **tkwargs):
        self.log(level, template, *targs, **tkwargs)
    return method

class LogMethodsMixin(object):
    def log(self, level, template, *targs, **tkwargs):
        if targs or tkwargs:
            self._log(level, template.format(*targs, **tkwargs))
        else:
            self._log(level, template)

    def level_func(self, level):
        return functools.partial(self.log, level)

    def _log(self, level, message):
        raise NotImplementedError()

    critical = log_method(CRITICAL)
    error = log_method(ERROR)
    warning = log_method(WARNING)
    info = log_method(INFO)
    debug = log_method(DEBUG)


class NoOpLogger(LogMethodsMixin):
    def log(self, level, template, *targs, **tkwargs):
        # override it here too so we don't spend time formatting
        pass

    def _log(self, level, message):
        pass


class Logger(LogMethodsMixin):
    def __init__(self, stream=None, min_level=DEBUG):
        self._stream = stream or sys.stdout
        self._min_level = min_level

    def _log(self, level, message):
        if level >= self._min_level:
            print(message, file=self._stream)
