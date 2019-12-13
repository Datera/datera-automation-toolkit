# -*- coding: utf-8 -*-
"""
Utility functions for cluster-wide operations

This is a high-level package, which depends on lower-level packages
such as qalib.api, qalib.components.*, etc.

Lower-level libraries should *not* call into this.
"""

from .clusterutil import Clusterutil
import qalib.api as _qalib_api

__copyright__ = "Copyright 2020, Datera, Inc."

def from_api(api):
    """
    Returns a Clusterutil object
    Parameter:
      api (qalib.api)
    """
    cluster = _qalib_api.get_clusterequipment(api)
    return Clusterutil(cluster)


def from_cluster(cluster):
    """
    Returns a Clusterutil object
    Parameter:
      cluster (qalib.corelibs.equipmentprovider.ClusterEquipment)
    """
    return Clusterutil(cluster)


__all__ = ['Clusterutil', 'from_api', 'from_cluster']
