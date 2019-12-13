# -*- coding: utf-8 -*-
"""
Provides the Clusterutil object
"""

import logging
import time
import random

import qalib.api
import qalib.clusterutil.health

__copyright__ = "Copyright 2020, Datera, Inc."
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class Clusterutil(object):
    """
    Utility functions for cluster-wide operations
    """
    def __init__(self, cluster, api=None):
        """
        Do not instantiate directly; use the package factory functions
        from_cluster() or from_api().
          cluster (qalib.equipment.ClusterEquipment)
        """
        self._cluster = cluster
        self.api2 = qalib.api.sdk_from_cluster(self._cluster)
        self.api = self.api2

        # These are lazily-loaded when the corresponding public property
        # is accessed:
        self._health = None

    @property
    def sw_version(self):
        """
        Cluster software version.

        Note: this property is not cached; each access generates a new
              query to the REST interface.
        """
        return str(self.api2.system.get()['sw_version'])

    @property
    def health(self):
        """ qalib.clusterutil.health """
        if not self._health:
            self._health = qalib.clusterutil.health.from_cluster(
                self._cluster, self.api)
        return self._health

    def is_cluster_version_greater_than_min(self, min_version=None):
        """
        Will flatten cluster version to an int, this is helpful when determing
        if a feature is supported based on the cluster version.
        Args:
            min_version: (str) cluster SW version ex, 3.1.2.
        Returns:
            True if required_version is higher than version running on the
            cluster.
        Examples:
            cluster_version = 3.1.1 min_version = 3.1.2 False
            cluster_version = 3.2.0 min_version = 3.1.2 True
            cluster_version = 2.2.4 min_version = 3.1.2 False
        """
        if min_version is None:
            raise ValueError("min_version is a required str argument")
        int_min_version = int(str(min_version).replace('.', '')[0:3])
        if int_min_version < 100:
            raise ValueError("min version must be at least 3 positions")
        # TODO handle versions greater than 10.0.0
        int_run_version = int(str(self.sw_version).replace('.', '')[0:3])
        if int_run_version >= int_min_version:
            return True
        else:
            return False

    def _do_cleanup_v21(self):
        """ API v2.1 cleanup"""
        api = self.api
        for ai in api.app_instances.list():
            logger.debug("Delete app_instance " + ai['name'])
            ai.set(admin_state='offline', force=True)
            ai.delete()
        for ig in api.initiator_groups.list():
            logger.debug("Delete initiator group " + ig.name)
            ig.delete()
        for i in api.initiators.list():
            logger.debug("Delete initiator " + i.name)
            i.delete()

        for tenant in api.tenants.list():
            name = ('/root/' + tenant['name']
                    if tenant['name'] != "root" else "/root")
            for ig in api.initiator_groups.list(tenant=name):
                logger.debug("Delete initiator group " + ig.name)
                ig.delete()
            for i in api.initiators.list(tenant=name):
                logger.debug("Delete initiator " + i.name)
                i.delete()
            for ai in api.app_instances.list(tenant=name):
                logger.debug("Delete app_instance " + ai['name'])
                ai.set(admin_state='offline', force=True, tenant=name)
                ai.delete(tenant=name)
            if tenant.name != "root":
                logger.debug("Delete tenant" + name)
                tenant.delete(tenant=name)

    def _do_cleanup_v2(self):
        """ API v2 cleanup """
        for ai in self.api.app_instances.list():
            print("Delete app_instance " + ai['name'])
            ai.set(admin_state='offline', force=True)
            ai.delete()

        for ig in self.api.initiator_groups.list():
            print("Delete initiator group " + ig.name)
            ig.delete()
        for i in self.api.initiators.list():
            print("Delete initiator " + i.name)
            i.delete()

    def force_clean(self):
        """
        Delete all volumes and exports on a cluster

        WARNING!! DANGER!! This destroys data!!

        this is tools/force_clean_cluster
        """
        # TODO[jsp]: if api_version_2_2 do threaded cleanup?
        self._do_cleanup_v21()
        time.sleep(2)
        self._do_cleanup_v2()

    def list_nodes(self, random_order=True, include_dead=False):
        """
        Returns list of active nodes in the cluster.
        @params:
          random_order (bool) - If True, return a randomly shuffled list
          include_dead (bool) - If False, exclude decommissioned nodes
        """
        node_list = []
        for node in self.api2.storage_nodes.list():
            if include_dead is False:
                # exclude dead nodes:
                if ((node.get('op_state', None) == 'decommissioned' or
                     node.get('op_state', None) == 'failed' or
                     node.get('op_state', None) == 'unknown' or
                     node.get('op_state', None) == 'dead')):
                    continue  # skip DEAD node
                if node['uuid'] == "00000000-0000-0000-0000-000000000000":
                    continue   # skip nodes not initialized yet
            node_list.append(node)
        if random_order:
            random.shuffle(node_list)
        return node_list
