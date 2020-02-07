#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Provides the ClusterReady class.

Use this class to verify that the cluster/clients/storage nodes
are ready for use
"""
import logging
import json

import qalib.whitebox as wb
import qalib.client
import qalib.clusterutil
from qalib.clients_can_reach_vips import can_client_reach_ips
from qalib.qabase.threading import Parallel

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

__copyright__ = "Copyright 2020, Datera, Inc."


class ClusterReady(object):
    """ Utilities for verifying cluster is ready for testing to begin """
    def __init__(self, equipment):
        self.cluster = equipment.get_cluster(required=True)
        self.clients = qalib.client.list_from_equipment(equipment,
                                                        required=True)
        self.cluster_util = qalib.clusterutil.Clusterutil(self.cluster)
        self.sdk = qalib.api.sdk_from_cluster(self.cluster)
        node_ips = self.cluster.get_server_ip_list()
        self.nodes = [wb.from_hostname(node,
                                       username=self.cluster.admin_user,
                                       password=self.cluster.admin_password)
                      for node in node_ips]


    def rotate_logs(self):
        """
        Force log rotation on all clients
        """
        def _force_log_rotation_on_client(client):
            logger.info("Forcing log rotation on client {}".format(client.name))
            client.force_log_rotation()
        funcs = list()
        args = list()
        for client in self.clients:
            funcs.append(_force_log_rotation_on_client)
            args.append([client])
        parent = Parallel(funcs=funcs, args_list=args, max_workers=len(funcs))
        parent.run_threads()


    def ensure_cluster_ready(self):
        """
        Run cluster health-checks required before beginning testing
        """
        logger.info("Waiting for all nodes to be online")
        self.cluster_util.health.wait_for_all_nodes_online()
        logger.info("Running network diagnostics")
        checks = ["interfaces", "ntp"]
        for check in checks:
            if not check_cluster_config_passes(self.nodes[0],
                                               check,
                                               additional_flags=["parallel"]):
                raise RuntimeError("check_cluster_config {} --parallel failed".
                                   format(check))
        logger.info("Can clients reach VIPs?")
        # TODO[jsp]: more of this can probably be
        # moved into can_client_reach_ips
        vips = list()
        for vip in self.sdk.system.network.access_vip.get()["network_paths"]:
            vips.append(vip["ip"])
        if not vips:
            raise RuntimeError("No VIPs detected on cluster")
        funcs = list()
        args = list()
        for client in self.clients:
            funcs.append(can_client_reach_ips)
            args.append([client, vips])
        parent = Parallel(funcs=funcs, args_list=args)
        parent.run_threads()

        logger.info("Can equipment be cleaned up?")
        logger.info("...cleaning clients")
        funcs = list()
        args = list()
        for client in self.clients:
            funcs.append(lambda c: c.force_cleanup_all())
            args.append([client])
            continue
        parent = Parallel(funcs=funcs, args_list=args,
                          max_workers=len(self.clients))
        parent.run_threads()
        logger.info("...cleaning cluster")
        self.cluster_util.force_clean()

        logger.info("All cluster health checks completed successfully")


# this is not necessarily a pre-flight fn.. consider moving it elsewhere?
def check_cluster_config_passes(node, check_type, additional_flags=None):
    """
    Runs check cluster config cmd on node and returns True if the check passes.
    Otherwise returns False

    Args:
      node: a whitebox object to run check_cluster_config on
      check_type (str): name of the check_cluster_config check to run
                        eg "all", "dns"
      additional_args (list of str): additional arguments eg ["--parallel"]
    """
    cmd = "check_cluster_config {} -o json".format(check_type)
    if additional_flags:
        cmd += " " + " ".join(additional_flags)
    output = node.cli.exec_command(cmd)
    health_report = json.loads(output)

    check_result = health_report[check_type]["result"]
    if not check_result:
        logger.error("Command {} failed\n{}".format(cmd, output))
    return check_result == "passed"
