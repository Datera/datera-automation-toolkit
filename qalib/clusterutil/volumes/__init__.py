# -*- coding: utf-8 -*-
"""
Provides access to volume utilities

Normally, this is accessed via the volumes property on clusterutil.
"""

from .volumes import Volumes
from .volumes import from_cluster

__all__ = ['Volumes', 'from_cluster']
