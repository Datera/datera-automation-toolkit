# -*- coding: utf-8 -*-
"""
Default product version information, this should be the items that would need
 to be overwritten in sub classes.
"""
import logging

from qalib.qabase.constants import GB, TB


__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())


class DefaultLimits(object):
    """Default limits_constants agnostic of version"""

    def __init__(self):
        pass
        # super(DefaultLimits, self).__init__()

    @property
    def number_of_nodes(self):
        return 20

    @property
    def number_of_tennants(self):
        return 64

    @property
    def number_of_app_instances(self):
        return 4096

    @property
    def number_of_storage_instances(self):
        return 2096

    @property
    def number_of_storage_instances_per_app_instance(self):
        return 256

    @property
    def max_volume_size_bytes(self):
        return 256 * TB

    @property
    def min_volume_size_bytes(self):
        return 1 * GB

    @property
    def number_of_volumes_system(self):
        return 4096

    @property
    def number_of_volumes_per_storage_instance(self):
        return 256

    @property
    def number_of_snapshots_system(self):
        return 16384

    @property
    def number_of_snapshots_per_volume(self):
        return 256

    @property
    def number_of_initiators(self):
        return 512

    @property
    def max_replicas(self):
        return 5

    @property
    def min_replicas(self):
        return 1

    @property
    def number_of_users(self):
        return 256

    @property
    def internal_volumes_per_client(self):
        return 256

    @property
    def fd_delay(self):
        return 3

    def get_all_limits_dict(self):
        """
        Returns: (dict) of all properties for limits object.
        """
        limits_dict = dict()
        for property in dir(self):
            if "__" in property or "get_all_limits_dict" in property:
                continue
            limits_dict[property] = self.__getattribute__(property)
        return limits_dict
