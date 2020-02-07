# -*- coding: utf-8 -*-
'''
Package for interacting with client systems.

Test cases should use from_equipment() or list_from_equipment() to
get Client object instances.

This package depends on qalib.api.
'''
__copyright__ = "Copyright 2020, Datera, Inc."

from .client import list_from_equipment
from .client import Client


__all__ = ['Client',
           'list_from_equipment',
           ]
