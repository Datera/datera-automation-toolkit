# -*- coding: utf-8 -*-
"""
Provides the NullOutput class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."


class NullOutput(object):
    """
    A fake file-like object which discards all output
    """

    def __init__(self):
        """ Set some dummy attributes """
        self.name = "<NULL>"
        self.mode = "w"
        self.closed = False

    @staticmethod
    def isatty():
        """ Don't try to treat this like a terminal """
        return False

    def _noop(self, *args, **kwargs):
        """ Method which accepts any arguments and does nothing """
        return None

    def __getattr__(self, attrname):
        """
        Turn normal file method calls (write, etc) into noops.
        """
        if hasattr(object, attrname):
            return getattr(object, attrname)  # __repr__, etc
        elif attrname == "fileno" or attrname.startswith("_"):
            raise AttributeError("%r object has no attribute %r" %
                                 (self.__class__.__name__, attrname))
        else:
            return self._noop
