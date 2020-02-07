#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Verifies connectivity from all clients to all IPS in the cluster.
"""
__copyright__ = "Copyright 2020, Datera, Inc."

import logging
import sys

import qalib.api
import qalib.client
import qalib.equipment

from qalib.qabase.threading import Parallel


logging.basicConfig()
logger = logging.getLogger()

def get_access_vips(opts):
    """Returns a list of IP addresses for the vips on a cluster"""
    cluster = opts.equipment.get_cluster()
    sdk = qalib.api.sdk_from_cluster(cluster)
    vips = list()
    for vip in sdk.system.network.access_vip.get()["network_paths"]:
        vips.append(vip["ip"])
    if not vips:
        logger.critical("No vips detected on cluster.")
        sys.exit(1)
    return vips


def get_all_ips(opts):
    """Returns a list of all IPS provisioned in cluster."""
    cluster = opts.equipment.get_cluster()
    sdk = qalib.api.sdk_from_cluster(cluster)
    ips = list()
    for ai in sdk.app_instances.list():
        for si in ai.storage_instances.list():
            ips.extend(si["access"]["ips"])
    return ips


def describe_cluster(cluster):
    logger.info("Cluster: {}".format(cluster.name))
    logger.info("  node names: " + repr(cluster.get_server_name_list()))
    logger.info("  node IPs:   " + repr(cluster.get_server_ip_list()))
    logger.info("  VIP:        " + repr(cluster.get_api_mgmt_vip()))


def can_client_reach_ips(client, ips):
    """Will raise if client can not reach vip"""
    failed_ips = list()
    funcs = list()
    args = list()
    for ip in ips:
        funcs.append(can_client_reach_ip)
        args.append([client, ip, failed_ips])
    Parallel(funcs=funcs, args_list=args).run_threads()
    if failed_ips:
        success_ips = (set(ips) - set(failed_ips)).union(
            set(failed_ips) - set(ips))
        msg = ("{}, is not able to ping {} ips, but is able to ping {} ips."
               "failed IPs:\n".format(
                client.name, len(failed_ips), len(success_ips)))
        for ip in failed_ips:
            msg += "{}\n".format(ip)
        logger.error(msg)
        raise ValueError(msg)
    else:
        msg = ("Client {} is able to ping {} ips.".format(client.name,
                                                          len(ips)))
        logger.info(msg)


def can_client_reach_ip(client, ip, failed_ips):
    """Method that pings from the client to the IP and logs.
    Will also add the IP address to a list if it fails."""
    ping_counts = [3, 5, 25]
    # speeding up ping checks
    ping_interval = 0.2
    success = False
    for ping_time in ping_counts:
        if not client.networking.ping(
                ip, count=ping_time, interval=ping_interval,
                partial=True, size=32000):
            msg = ("Endpoint addr not reachable from {}: {} with ping"
                   " count of {}.".format(
                    client.name, ip, ping_time))
            logger.error(msg)
        else:
            msg = ("Endpoint addr is reachable from {}: {} with ping"
                   " count of {}.".format(
                    client.name, ip, ping_time))
            logger.debug(msg)
            success = True
            break
    if not success:
        failed_ips.append(ip)


def clients_can_reach_ips(opts):
    """
    Threads each Client for all IPS
    """
    ips = get_access_vips(opts)
    if opts.all_ips:
        ips.extend(get_all_ips(opts))
    funcs = list()
    args = list()
    for client in qalib.client.list_from_equipment(opts.equipment,
                                                   required=False):
        funcs.append(can_client_reach_ips)
        args.append([client, ips])
    parent = Parallel(funcs=funcs, args_list=args)
    try:
        parent.run_threads()
        return True
    except ValueError:
        cluster = opts.equipment.get_cluster()
        describe_cluster(cluster)
        return False


def main():
    """ Parse args """
    description = "Verifies clients can ping cluster vips."
    parser = qalib.equipment.get_argument_parser(description=description)
    parser.add_argument("-v", "--verbose", dest='verbosity', action='count',
                        help="Show debug logging",
                        default=1)
    parser.add_argument("-all", dest="all_ips", action="store_true",
                        help="Ping all available IPs from all clients.",
                        default=False)
    opts = parser.parse_args()
    if opts.equipment is None:
        parser.error("No equipment specified")

    if clients_can_reach_ips(opts):
        logger.info("Done")
        return 0
    else:
        return 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(1)
