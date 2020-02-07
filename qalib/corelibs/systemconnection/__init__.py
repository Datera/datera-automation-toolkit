# -*- coding: utf-8 -*-
'''
Provides the SystemConnection object and related factory functions

The SystemConnection object provides communication/control of remote
systems.  It is the lower communication layer that Client and Whitebox
objects rely on.

It is currently based on XMLRPC.

Tests and tools should not use this package directly.
'''
__copyright__ = "Copyright 2020, Datera, Inc."

from .sysconn import SystemConnection
from .sysconn import from_hostname

__all__ = ['SystemConnection', 'from_hostname']
