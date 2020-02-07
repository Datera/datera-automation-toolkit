# -*- coding: utf-8 -*-
"""
Bridge between this package and the nose package
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

__copyright__ = "Copyright 2020, Datera, Inc."


def _prepare_sys_path():
    """
    Raises ImportError on failure.

    Tries to import nose using the default sys.path; if that fails, adds
    our third_party/ folder to sys.path and tries again.
    """
    try:
        import nose
        return
    except ImportError:
        pass
    import os
    import sys
    thisdir = os.path.dirname(os.path.realpath(__file__))
    third_party_libdir = os.path.join(thisdir, "third_party")  # TODO[jsp]: necessary?
    sys.path.insert(0, third_party_libdir)
    import nose
_prepare_sys_path()


import nose
import nose.plugins
from nose.plugins.attrib import attr
