# -*- coding: utf-8 -*-
"""
Provides methods for ensuring that a cluster is in an acceptable health state
"""
import logging

import qalib.api
import qalib.clusterutil
from qalib.qabase.threading import Parallel
from qalib.qabase.polling import result_poll
from qalib.qabase.exceptions import PollingException

__copyright__ = "Copyright 2020, Datera, Inc."
logger = logging.getLogger(__name__)

class Health(object):
    """
    Library checking health on a cluster
    """

    def __init__(self, cluster, api):
        self._cluster = cluster
        if api is None:
            self._api = qalib.api.sdk_from_cluster(self._cluster, developed_for='v2.2')
        else:
            self._api = api
        self._clusterutil = qalib.clusterutil.from_cluster(cluster)
        self._log_level = logging.DEBUG

    def check_if_node_online(self, node):
        """
        Queries API to determine op_state of `node` and returns True the value
        is "online"
        """
        state = self._api.storage_nodes.get(node.uuid).get("op_state", None)
        if state:
            logger.debug("Node {} op_state={}".format(node.uuid, node.op_state))
        return state == True

    def wait_for_node_online(self, node, timeout=600, interval=10):
        """
        Waits for node to be in op_state=online.
        Polls until this state is reached or until timeout.

        Params:
            timeout (int): Time, in seconds, to wait for node
            interval (int): Time, in seconds, between each check of op_state
        """
        if node.get("op_state", None) == "online":
            logger.info("Node {} is already online".format(node.name))
        else:
            try:
                result_poll(function=self.check_if_node_online,
                            args=[node],
                            expected_result=True,
                            timeout=timeout, interval=interval)
                logger.info("Node {} is now online".format(node.name))
            except PollingException as err:
                raise PollingException("Node {} not up after {} seconds".format(
                    node.name, timeout))

    def wait_for_all_nodes_online(self):
        """
        Waits until all nodes in the cluster are in running or recovered.
        """
        nodes = self._clusterutil.list_nodes()
        funcs_list = list()
        args_list = list()
        for node in nodes:
            funcs_list.append(self.wait_for_node_online)
            args_list.append([node])
        parent = Parallel(funcs=funcs_list, args_list=args_list)
        parent.run_threads()
        logger.info("All nodes are online")

def from_cluster(cluster, api=None):
    return Health(cluster, api)
