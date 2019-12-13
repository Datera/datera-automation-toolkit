# -*- coding: utf-8 -*-
'''
Provides whitebox access to storage nodes.

Example: Execute a command on every server node:

    for node in api.storage_nodes.list():
        whitebox = qalib.whitebox.from_node(node)
        output = whitebox.exec_cmd_check("dcp -l")

This package depends on the qalib.api package.
'''

from .box import Whitebox

__copyright__ = "Copyright 2020, Datera, Inc."


def from_node(node):
    '''
    Returns a Whitebox object connected to the given storage node.

    Parameter:
      node - a storage_node Api object
    '''
    return Whitebox.from_node(node)


def from_hostname(hostname, username=None, password=None):
    '''
    Returns a Whitebox object.
    This is mainly intended for debugging.
    Tests and tools should normally use from_node()

    Parameters:
      hostname (str)
      username (str) - e.g. "root"
      password (str) - e.g. "my_root_pass"
    '''
    return Whitebox.from_hostname(hostname,
                                  username=username, password=password)


# TODO: @deprecated_by("list_from_cluster")
def list_from_equipment(equipment):
    """
    Deprecated.  Do not use.

    Use list_from_cluster() or from_node() instead.
    """
    whitebox_list = []
    for cluster in equipment.get_cluster_list(required=False):
        whitebox_list.extend(list_from_cluster(cluster))
    return whitebox_list


def list_from_cluster(clusterequipment):
    """
    Returns a list of Whitebox objects obtained from a cluster equipment
    object.

    Note: this is not the normal recommended way to get whitebox objects.
    Normally, you should connect to the cluster API, get a list of nodes,
    and instantiate Whitebox objects using from_node().
    This returns all nodes defined by the equipment, whether or not they
    are actually part of the DCP cluster.
    For example, if you have a 6-node cluster and you decommission one
    node, then the API will show you 5 alive nodes.  But list_from_cluster()
    will return whitebox objects for 6 nodes, including the decommssioned
    node.

    Parameter:
      clusterequipment (qalib.corelibs.equipmentprovider.ClusterEquipment)
    Raises qalib.corelibs.equipmentprovider.EquipmentNotFoundError if no
    cluster is defined.
    """
    whitebox_list = list()
    for servername in clusterequipment.get_server_name_list():
        sysconn = clusterequipment.get_server_systemconnection(servername)
        whitebox = Whitebox.from_connection(sysconn)
        whitebox.on_hardware = clusterequipment.is_on_hardware()
        whitebox.name = servername
        whitebox_list.append(whitebox)
    return whitebox_list

__all__ = ['Whitebox', 'from_node', 'from_hostname', 'list_from_cluster']
