#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import qalib.api
import qalib.clusterutil

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class Features(object):
    """
    This is Features class which contains information about features
    to be enabled if cluster supports them

    """
    # 2.x
    compression_in_band_min_version = "2.2.0"
    # 3.1.x
    stretch_cluster_min_version = "3.1.8"
    # 3.0.x
    api_version_2_2_min_version = "3.0.0"
    failure_domain_min_version = "3.0.0"
    inode_verify_min_version = "3.0.0"
    # 3.2.x
    discard_min_version = "3.2.2"
    sub_60_second_dual_fail = "3.2.3"
    drive_threshold_limit_version = "3.2.3"
    failure_domain_alert_deleted_min_version = "3.2.7"

    # 3.3.x
    stretch_witness_latest_version = "3.3.0"
    dedupe_in_band_min_version = "3.3.0"
    node_personality_min_version = "3.3.0"
    core_on_no_snapshot_min_version = "3.3.0"
    api_events_min_version = "3.3.0"
    vol_state_placement_mismatch = "3.3.1"
    disable_manual_snaps_min_version = "3.3.3"

    def __init__(self, cluster=None, api=None):
        if cluster is None and api is None:
            raise ValueError("Features requires either an API"
                             " or a cluster object.")
        if cluster is None:
            self.clusterutil = qalib.clusterutil.from_api(api)
            self.api = api
        if api is None:
            self.clusterutil = qalib.clusterutil.from_cluster(cluster)
            # self.api = qalib.api.from_cluster(cluster, developed_for="v2")
            self.api = qalib.api.sdk_from_cluster(cluster)

    @property
    def api_version_2_2(self):
        """
        Check if the current version supports version 2.2 of the API.  In
        earlier product versions this was not exported via the api.
        """
        if self.clusterutil.is_cluster_version_greater_than_min(
                min_version=self.api_version_2_2_min_version):
            return True
        else:
            msg = ("Cluster Version: {}. API version 2.2 not available "
                   "version requirement: {}".format(
                    self.software_version, self.api_version_2_2_min_version))
            logger.warning(msg)
            return False

    @property
    def failure_domains(self):
        """
        Check if the current version supports failure domains.
        """
        if self.clusterutil.is_cluster_version_greater_than_min(
                min_version=self.failure_domain_min_version):
            return True
        else:
            msg = ("Cluster Version: {}. failure domains not available "
                   "version requirement: {}".format(
                    self.software_version, self.failure_domain_min_version))
            logger.warning(msg)
            return False

    @property
    def inode_verify(self):
        """
        Check if the current version supports data checking inode_verify tool.
        """
        if self.clusterutil.is_cluster_version_greater_than_min(
                min_version=self.inode_verify_min_version):
            return True
        else:
            msg = ("Cluster Version: {}. inode_verify not available "
                   "version requirement: {}".format(
                    self.software_version, self.inode_verify_min_version))
            logger.warning(msg)
            return False

def from_cluster(cluster):
    """
    Returns a features instance for determining supported features
    based on currently running product version.

    Args: Pass cluster object
    """

    return Features(cluster=cluster)

def from_api(api):
    """
    Returns a features instance for determining supported features
    based on currently running product version.

    Args: Pass api object
    """

    return Features(api=api)
