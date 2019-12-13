# -*- coding: utf-8 -*-
"""
QA Automation Base Package

Base package for the Automation framework.  This package is not allowed to
have any dependencies on any other Automation framework package, but is
allowed to have 3rd party dependencies.

The purposed of this package is to hold classes and functions that are
needed by all the rest of the Automation packages.  Exceptions should go
here first before anywhere else so they're available for Unit Tests to import.
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
