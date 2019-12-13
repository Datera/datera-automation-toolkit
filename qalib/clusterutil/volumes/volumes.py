# -*- coding: utf-8 -*-
""" Provides the Volumes class """

import logging

from qalib.qabase.exceptions import ApiNotFoundError

__copyright__ = "Copyright 2020, Datera, Inc."

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class Volumes(object):
    """
    Utilities for querying/manipulating volumes
    """
    _api_version = None

    def __init__(self, cluster, api=None):
        self._cluster = cluster
        if self._api_version is None:
            # sub-classes must define self._api_version
            raise NotImplementedError("Invalid object")
        if api is None:
            self._api = qalib.api.sdk_from_cluster(self._cluster,
                        version=self._api_version)
        else:
            self._api = api
        self._placement = None
        self._io = None
        self._span = None
        self._dataverification = None
        self._media_personality = None
        self._media_placement = None
        self._health = None

class Volumes_v2_1(Volumes):
    _api_version = 'v2.1'

    def list_all_vols_from_ai_list_safe(self, ai_list=None):
        """Returns Vol list from app instance list with error handling"""
        vol_list = list()
        for ai in ai_list:
            try:
                for si in ai.storage_instances.list():
                    try:
                        for vol in si.volumes.list():
                            vol_list.append(vol)
                    except ApiNotFoundError:
                        # vol no longer exists
                        continue
            except ApiNotFoundError:
                # ai no longer exists
                continue
        return vol_list


def from_cluster(cluster):
    """ Returns a Volumes instance """
    # TODO: need to detect the correct class to return
    return Volumes_v2_1(cluster)
