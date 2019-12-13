# -*- coding: utf-8 -*-
"""
Datera test package

This skeleton code demonstrates how this package should be used to
write a test:

    #!/usr/bin/env python
    import qatest
    class TestCase(qatest.TestCase):
        def setUp(self):
            pass
        def tearDown(self):
            pass
        def test_run(self):
            pass
    if __name__ == '__main__':
        qatest.main()

This package depends on the qalib.corelibs package.
This package depends on the qahealth package.
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."


from .case import TestCase
from .nose_bridge import attr


__all__ = ['TestCase',
           'attr']
