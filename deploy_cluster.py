# -*- coding: utf-8 -*-
"""
Deploy a cluster. Also performs some basic checks on the
cluster pre-deployment (i.e. on the individual equipment) and post-deployment.

Example usage:
$ python deploy_cluster.py --init my_cluster_initfile.json --cluster my_cluster_description.json
"""
__copyright__ = "Copyright 2020, Datera, Inc."

import argparse
import dfs_sdk
import json
import logging
import paramiko.ssh_exception
import requests
import socket
import sys
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from qalib.corelibs.ssh import SSH
from qalib.qabase.log import TOOL_LOGSCREEN_FORMAT, LOGFILE_FORMAT
from qalib.qabase.threading import Parallel
import qalib.api
import qalib.client
import qalib.clusterutil
import qalib.equipment
import qalib.preflight
import qalib.whitebox as wb


logging.basicConfig()
logger = logging.getLogger()

def log_exceptions(exc_type, exc_value, exc_traceback):
    """"
    Hook for logging any unhandled exceptions
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.critical("Encountered an exception", exc_info=(exc_type,
                                                           exc_value,
                                                           exc_traceback))

def setup_logging():
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.Formatter.converter = time.gmtime
    rootlogger = logging.getLogger()
    rootlogger.setLevel(logging.DEBUG)
    # remove other handlers
    for handler in rootlogger.handlers:
        rootlogger.removeHandler(handler)
    # log >= INFO to screen
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(TOOL_LOGSCREEN_FORMAT)
    handler.setFormatter(formatter)
    rootlogger.addHandler(handler)
    # log everything to file
    now = time.time()
    filename = "datera_cluster_deploy-" + str(int(now)) + ".txt"
    handler = logging.FileHandler(filename, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOGFILE_FORMAT)
    handler.setFormatter(formatter)
    rootlogger.addHandler(handler)
    logger.info("Logging to " + filename)

def _read_init(init):
    """
    Returns cluster initfile from commandline value

    Args:
      init: a string or a file path
    Returns a string
    """
    if init.lower().endswith(".json"):
        with open(init, 'r') as fp:
            return fp.read()
    elif isinstance(init, basestring):
        return init
    else:
        raise ValueError("Cannot create cluster initfile from {}" % type(init))

# TODO[jsp]: may be worth moving this into main library?
class _ClusterInitArgparseAction(argparse.Action):
    """ Parses the --init argument """
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            setattr(namespace, self.dest, _read_init(values))
        except ValueError as ex:
            raise argparse.ArgumentError(self, str(ex))

def get_args():
    """
    Parses and return commandline arguments
    """
    equipment_parser = \
        qalib.equipment.get_argument_parser(add_help=False)
    parser = argparse.ArgumentParser(parents=[equipment_parser])
    parser.add_argument("init",
                        help="Path to cluster init file (ending in .json)",
                        action=_ClusterInitArgparseAction)
    args = parser.parse_args()
    return args

def _get_api_headers():
    """
    Returns a dict of the api headers needed to talk to api pre-cluster init
    """
    return {"content-type": "text/plain;charset=UTF-8"}

def deploy_cluster(init_conf, node):
    """
    Deploy a cluster.

    This cluster only sends the initialize request but does not check if/when
    the cluster initializes correctly.

    Args:
      cluster (qalib.equipment.clusterEquipment.ClusterEquipment):
        the cluster to deploy
      init_conf (str): cluster initiliazation conf
      node (str): the node to send cluster init request to
    """
    init_path = "http://{}:8500/v2/create".format(node.ip)
    response = requests.post(init_path,
                             data=init_conf,
                             headers=_get_api_headers())
    if response.content != "create":
        try:
            json_response = response.json()
        except ValueError:
            json_response = None
        msg = ("Cluster init request to node {} failed with HTTP {}".format(
            node.name, response.status_code))
        if json_response:
            msg += "\n" + json.dumps(json_response, indent=4, sort_keys=True)
        elif response.content:
            logger.debug("Cluster create response\n{}".format(response.content))
        else:
            pass
        raise RuntimeError(msg)

def join_cluster(cluster_name, node, requests_session):
    """
    Join a node to an existing cluster.

    This function just only sends the join request but does not check if/when
    the node joins.

    Args:
      cluster_name (str)
      node (str): the node to join to the cluster
    """
    logger.debug("Joining {}".format(node.name))
    api_path = "http://{}:8500/v2/join".format(node.ip)
    body = """{
        "system": {
            "name": "%s"
        }
    }""" % (cluster_name,)
    response = requests_session.post(api_path,
                             data=body,
                             headers=_get_api_headers())
    # a successful request returns "join" as response body
    if response.content != "join":
        if response.content:
            logger.debug("Cluster join failed with response\n{}".format(
                response.content))
        else:
            logger.debug("Cluster join failed with no response body")
        raise RuntimeError("Join request to node '{}' failed with HTTP {}".format(
node.name, response.status_code))

def get_cluster_api(cluster, timeout=600):
    """
    Returns cluster's sdk object

    Raises RuntimeError if we can't get an api object within 10 minutes

    Args:
      cluster (qalib.equipment.clusterEquipment.ClusterEquipment)
      timeout (int): number of seconds to attempt to connect to api
    """
    sdk = None
    wait = 10 # time to wait between tries
    end = time.time() + timeout
    while time.time() < end:
        try:
            sdk = qalib.api.sdk_from_cluster(cluster)
            break
        except dfs_sdk.exceptions.ApiConnectionError as err:
            logging.error("API connection failed due to error {}"
                          "\nSleeping {} and will retry".format(str(err), wait))
            time.sleep(wait)
    if sdk is None:
        raise RuntimeError("Unable to access cluster API after {} seconds".format(timeout))
    return sdk

def wait_for_cluster(api, expected_nodes, timeout=1800):
    """
    Wait up to half an hour for cluster to form correctly

    api: datera sdk object used to interface with the cluster
    expected_nodes (list of str): IPs of nodes that should be in the cluster
    timeout (int): number of seconds to wait for cluster to form
    """
    stop_waiting = time.time() + timeout
    read_endpt = api.context.connection.read_endpoint
    sticking_point = None # cluster check that we haven't moved past yet
    while True:
        # sleeping first because the cluster is probably not ready
        # when this gets called
        time.sleep(10)
        system_status = read_endpt("/system")
        # is cluster running?
        if system_status["op_state"] != "running":
            sticking_point = "Cluster state is {}".\
                format(system_status["op_state"])
            logger.debug(sticking_point)
            continue
        api_nodes = read_endpt("/storage_nodes")
        if len(api_nodes) > len(expected_nodes):
            raise RuntimeError("There are {} nodes in cluster but expected only {}".\
                               format(len(expected_nodes), len(api_nodes)))
        elif len(api_nodes) < len(expected_nodes):
            sticking_point = "{} out of {} nodes have joined the cluster".\
                         format(len(api_nodes), len(expected_nodes))
            logger.debug(sticking_point)
            continue
        else:
            pass
        # make sure the nodes in the cluster are the same as nodes
        # we expect to be in cluster
        api_nodes_ips = map(lambda n: socket.gethostbyname(n["name"]), api_nodes)
        for ip in api_nodes_ips:
            if not ip in expected_nodes:
                raise RuntimeError("Unexpected node {} in cluster".format(ip))
        # are all nodes ready?
        num_nodes_running = 0
        for node in api_nodes:
            logger.debug("Node {} op_state is {}".format(node, node["op_state"]))
            if node["op_state"] == "online":
                # TODO[jsp]: possible to return earlier
                # if op_state in error or similar?
                num_nodes_running += 1
        if num_nodes_running == len(expected_nodes):
            return True
        else:
            sticking_point = "{} out of {} nodes are online".\
                format(num_nodes_running, len(expected_nodes))
        # make sure the nodes in the cluster are the ones we expect!
        if time.time() > stop_waiting:
            logger.error("Cluster did not form correctly: {}".\
                         format(sticking_point))
            return False

def make_parallel(fn, args, results):
    def run_fn(fn, arg, result_list):
        if fn(arg) is False:
            result_list.append(arg)
        return
    results = list()
    funcs = [run_fn for arg in args]
    args = [(fn, arg, results) for arg in args]
    return Parallel(funcs, args_list=args)

def main():
    opts = get_args()

    sys.excepthook = log_exceptions
    setup_logging()

    requests.packages.urllib3.disable_warnings(
        requests.packages.urllib3.exceptions.InsecureRequestWarning)
    logging.getLogger("requests").setLevel(logging.DEBUG)

    logger.info("Reading cluster details...")
    cluster = opts.equipment.get_cluster(required=True)
    node_ips = cluster.get_server_ip_list()
    nodes = map(lambda n: wb.from_hostname(n,
                                           username=cluster.admin_user,
                                           password=cluster.admin_password),
                node_ips)
    clients = qalib.client.list_from_equipment(opts.equipment,
                                               required=False)

    # BEGIN PRE-CLUSTER SETUP
    logger.info("Beginning pre-deploy checks")
    logger.info("Are clients reachable?")
    unpingable_clients = list()
    parent = make_parallel(lambda c: c.is_pingable(),
                           clients, unpingable_clients)
    parent.run_threads()
    if unpingable_clients:
        raise RuntimeError("Couldn't ping these client(s): {}".\
                           format(", ".join(unpingable_clients)))

    logger.info("Are we able to login on client systems?")
    def signin_client(client):
        # this is basically client.run_cmd
        shell = client._system_conn._get_shell()
        try:
            shell.exec_command("true")
            return True
        except paramiko.ssh_exception.AuthenticationException as _err:
            return False
    login_failed_clients = list()
    parent = make_parallel(signin_client, clients, login_failed_clients)
    parent.run_threads()
    if login_failed_clients:
        raise RuntimeError("Unable to login to client(s): {}".\
                           format(", ".join(login_failed_clients)))

    logger.info("Installing required packages on clients")
    funcs = list()
    args = list()
    for client in clients:
        funcs.append(lambda c: c.setup())
        args.append([client])
    parent = Parallel(funcs=funcs, args_list=args)
    parent.run_threads()

    logger.info("Are storage nodes reachable?")
    unpingable_nodes = list()
    parent = make_parallel(lambda n: n.is_pingable(), nodes, unpingable_nodes)
    parent.run_threads()
    if unpingable_nodes:
        raise RuntimeError("Unable to ping node(s): {}".format(
            ", ".join(unpingable_nodes)))

    logger.info("Can we sign-in to all nodes as admin?")
    failed_login_nodes = list()
    def login_node(node):
        ssh = SSH(node.ip,
                  username=cluster.admin_user,
                  password=cluster.admin_password)
        try:
            tp = ssh._open_transport()
            tp.close()
            return True
        except paramiko.ssh_exception.AuthenticationException:
            return False
    parent = make_parallel(login_node, nodes, failed_login_nodes)
    parent.run_threads()
    if failed_login_nodes:
        raise RuntimeError("Authentication failed: {}".format(
            ", ".join(failed_login_nodes)))

    logger.info("Is REST interface up for all nodes?")
    def is_rest_up(node, max_tries=3, wait_time=10):
        tries = 0
        path = "http://{}:8500".format(node.ip)
        # TODO[jsp]: probably smarter to allow requests to handle max tries
        # and backoff/wait time
        while tries < max_tries:
            tries += 1
            try:
                resp = requests.get(path)
                if resp.status_code == 200:
                    return True
            except requests.exceptions.ConnectTimeout as err:
                logger.debug(str(err))
            time.sleep(wait_time)
        return False
    rest_not_up = list()
    parent = make_parallel(is_rest_up, nodes, rest_not_up)
    parent.run_threads()
    if rest_not_up:
        raise RuntimeError("REST interface not up: {}".format(
            ", ".join(rest_not_up)))

    logger.info("Verifying cluster network")
    def check_ifaces(node):
        return qalib.preflight.check_cluster_config_passes(node, "interfaces")
    nodes_w_network_problems = list()
    parent = make_parallel(check_ifaces, nodes, nodes_w_network_problems)
    parent.run_threads()

    logger.info("Pre-deploy work complete!")

    logger.info("Deploying cluster")
    deploy_cluster(opts.init, nodes[0])
    if len(nodes) > 1:
        funcs = list()
        args = list()
        requests_session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[ 111 ])
        # TODO[jsp]: consider also using this adapter for init
        adapter = HTTPAdapter(max_retries=retries)
        requests_session.mount("http://", adapter)
        for node in nodes[1:]:
            funcs.append(join_cluster)
            args.append([cluster.name, node, requests_session])
        parent = Parallel(funcs=funcs, args_list=args)
        parent.run_threads()
    logger.info("Waiting for cluster to form")
    sdk = get_cluster_api(cluster)
    wait_for_cluster(sdk, node_ips)
    logger.info("Cluster formed correctly.")

    logger.info("Beginning post-deploy checks.")
    preflight_helper = qalib.preflight.ClusterReady(opts.equipment)
    preflight_helper.ensure_cluster_ready()
    logger.info("Your cluster is ready.")

if __name__ == "__main__":
    main()
