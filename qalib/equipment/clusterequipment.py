# -*- coding: utf-8 -*-
'''
This module provides the ClusterEquipment class and factory functions
'''
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import logging
import threading

from qalib.qabase.networking import gethostbyname_retry
from qalib.corelibs.credentials import from_user_pass
import qalib.corelibs.systemconnection

__copyright__ = "Copyright 2020, Datera, Inc."

#  LOG base
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class ClusterEquipment(object):  # pylint: disable=abstract-class-little-used

    """
    Cluster equipment specification (base class)

    Tests and tools should not interact with this directly.

    This is used by libraries which need to get information about cluster
    test equipment.  For example, if you had an ExampleUI class for talking
    to the REST API or something like that, the exampleui package might
    provide ExampleUI instances with something like this:

      def from_equipment(equipment):
          clusterequipment = equipment.get_cluster()
          mgmt_ip = clusterequipment.get_api_mgmt_vip()
          return ExampleUI(mgmt_ip)
    """
    def __init__(self, admin_creds=None,
                 mgmt_vip_addr=None,
                 name=None):
        """
        Don't instantiate this directly; use a factory function (below) to
        get a sub-class.
        Subclasses must call this __init__().
        """
        if not name:
            name = "cluster"
        self.name = name
        self._mgmt_vip_addr = mgmt_vip_addr
        # For now, don't accept a cluster without a management VIP.
        # We may want to relax this in the future, though.
        if mgmt_vip_addr is None:
            raise ValueError("Cluster %r has no management VIP" % name)
        # Credentials
        self._admin_creds = admin_creds
        self._util = None

        self._lock = threading.Lock()


    def to_dict(self):
        """ Return a dict representation of this object """
        cluster_data = {}
        cluster_data['name'] = self.name
        cluster_data['cluster_type'] = self.cluster_type
        cluster_data['admin_username'] = self._admin_creds.get_username()
        cluster_data['admin_password'] = self._admin_creds.get_password()
        cluster_data['mgmt_vip_addr'] = self._mgmt_vip_addr
        cluster_data['node_hostname_list'] = self.get_server_name_list()
        return cluster_data

    def get_admin_creds(self):
        """ Returns a Credentials object for talking to the API """
        return self._admin_creds

    # Sub-classes must implement these:

    @staticmethod
    def is_on_hardware():
        """ Returns True if this is a HW cluster, else False """
        raise NotImplementedError("Invalid object called")

    @staticmethod
    def is_on_vm():
        """ Returns True if this is a VM cluster, else False """
        raise NotImplementedError("Invalid object called")

    def get_server_name_list(self):
        """
        Returns a list of hostnames or IPs for each server node
        This function may return a resolvable hostname like "rts41", or
        it may return a name like "node1".
        You should generally prefer get_server_ip_list() instead of this.
        """
        raise NotImplementedError("Invalid object called")

    def get_server_ip_list(self):
        """ Returns a list of IPs for each server node """
        raise NotImplementedError("Invalid object called")

    def get_api_mgmt_vip(self):
        """ Returns a mgmt vip for REST queries """
        return self._mgmt_vip_addr

    def get_server_systemconnection(self, servername):
        """
        For library use only.  Tests and tools should not call this.

        Returns a system connection to the server node.  (This system
        connection will be used to instantiate a Whitebox object.)
        Parameter:
          servername (str) - e.g. 'rts41.daterainc.com'
        Returns: qalib.corelibs.systemconnection.SystemConnection
        """
        raise NotImplementedError("Invalid object called")

    ####################

    @property
    def mgmt_node(self):
        """ Returns the VIP. """
        return self.get_api_mgmt_vip()

    @property
    def num_nodes(self):
        """ The number of nodes in the cluster """
        return len(self.get_server_ip_list())

    @property
    def node_ip_list(self):
        """ List of IPs for controlling server nodes """
        return self.get_server_ip_list()

    @property
    def node_name_list(self):
        """ List of server node names (not necessarily resolveable) """
        return self.get_server_name_list()

    @property
    def admin_user(self):
        """ API username """
        return self.get_admin_creds().get_username()

    @property
    def admin_password(self):
        """ API password """
        return self.get_admin_creds().get_password()

    @property
    def on_hardware(self):
        """ Boolean, True for real clusters, False for actest """
        return self.is_on_hardware()

    @property
    def cli_user(self):
        """ Used to SSH into the CLI """
        return self.get_admin_creds().get_username()

    @property
    def cli_password(self):
        """ Used to SSH into the CLI """
        return self.get_admin_creds().get_password()

    @property
    def util(self):
        """ A qalib.clusterutil.Clusterutil instance """
        if self._util is None:
            import qalib.clusterutil  # pylint: disable=redefined-outer-name
            self._util = qalib.clusterutil.from_cluster(self)
        return self._util


class HWClusterEquipment(ClusterEquipment):

    """
    equipment specification for a hardware cluster
    """
    cluster_type = "hardware"

    def __init__(self, hostname_list, **kwargs):
        """
        Parameters:
          hostname_list (list) - A list of debug hostnames/IPs for each node
        See the superclass for additional keyword parameters.
        """
        self._hostname_list = hostname_list
        super(HWClusterEquipment, self).__init__(**kwargs)

    def copy(self):
        return self.__class__(self._hostname_list,
                              name=self.name,
                              admin_creds=self._admin_creds,
                              mgmt_vip_addr=self._mgmt_vip_addr)

    @staticmethod
    def is_on_hardware():
        """ Returns True if this is a hardware cluster, else False """
        return True

    @staticmethod
    def is_on_vm():
        """ Returns True if this is a vm cluster, else False """
        return False

    def get_server_name_list(self):
        """ Returns a list of hostnames or IPs for each server node """
        return list(self._hostname_list)

    def get_server_ip_list(self):
        """ Returns a list of IPs for each server node """
        return [gethostbyname_retry(hostname)
                for hostname in self._hostname_list]

    def _get_hostname_from_servername(self, servername):
        # Until DAT-3199 is fixed:
        if servername.endswith("(none)"):
            servername = servername[:-len(".(none)")]
        if "." not in servername:
            for host in self._hostname_list:
                if servername == host.split(".")[0]:
                    hostname = host
                    break
            else:
                hostname = servername
        else:
            hostname = servername
        return hostname

    # TODO[jsp]: this should be utilized instead of <wb>._sys_conn
    def get_server_systemconnection(self, servername):
        """
        For library use only.  Tests and tools should not call this.

        Returns a system connection to the server node.  (This system
        connection will be used to instantiate a Whitebox object.)
        Parameter:
          servername (str) - e.g. 'rts41.daterainc.com'
        Returns: qalib.corelibs.systemconnection.SystemConnection
        """
        creds = self._admin_creds
        # Until DAT-3199 is fixed:
        hostname = self._get_hostname_from_servername(servername)

        ip = gethostbyname_retry(hostname)
        return qalib.corelibs.systemconnection.from_hostname(ip,
                                                             creds=creds,
                                                             name=servername)


########################################

def from_dict(cluster_data, qa_equipment_schema_version=None):
    """
    Instantiate a cluster object from a dictionary, which is usually
    a subsection of a JSON config file.

    Example:
        {
            "name": "110s",
            "cluster_type": "hardware",
            "admin_username": "admin",
            "admin_password": "password",
            "mgmt_vip_addr": "172.16.6.110",
            "node_hostname_list": [
                "rts110",
                "rts111",
                "rts112"
            ]
        }
    """
    if str(qa_equipment_schema_version) != "1.0":
        raise NotImplementedError("Unknown schema version: %r" %
                                  qa_equipment_schema_version)

    if cluster_data.get("cluster_type", None) != "hardware":
        raise ValueError("Cluster must have cluster_type=hardware")

    node_hostname_list = cluster_data.get("node_hostname_list", [])
    if not node_hostname_list:
        raise ValueError("Cluster must have node_hostname_list populated")

    name = cluster_data.get("name", "cluster")

    admin_creds = None
    admin_username = cluster_data.get("admin_username", "admin")
    admin_password = cluster_data.get("admin_password", None)
    if admin_username and admin_password:
        admin_creds = from_user_pass(admin_username, admin_password)

    mgmt_vip_addr = cluster_data.get("mgmt_vip_addr", None)
    if not mgmt_vip_addr:
        raise ValueError("Cluster must have mgmt_vip_addr defined")

    return HWClusterEquipment(node_hostname_list,
                              name=name,
                              admin_creds=admin_creds,
                              mgmt_vip_addr=mgmt_vip_addr)
