# -*- coding: utf-8 -*-
"""
This module provides the EquipmentProvider class
"""
from __future__ import (unicode_literals, print_function)

import json

from qalib.qabase.exceptions import EquipmentNotFoundError
from . import clusterequipment
from . import clientequipment

__copyright__ = "Copyright 2020, Datera, Inc."


class EquipmentProvider(object):
    """
    This object provides lab equipment information to other libraries.
    It starts out empty, and needs to be populated by using the
    package-private _load_from_reservation_db_label() method.

    Tests and tools should treat this as an opaque data type.
    """

    def __init__(self):
        """
        Initializes an empty EquipmentProvider.
        Should only be instantiated from within this package.
        Equipment is added via the _load_from_*() methods.
        """
        self._cluster_list = []
        self._client_list = []

    def __nonzero__(self):
        """
        Treat this object as a bool; True if any equipment has been loaded
        into this object, False if it's empty (contains no equipment).
        """
        if self._cluster_list or self._client_list:
            return True
        return False
        # False means this EquipmentProvider is empty; no equipment has
        # been loaded yet

    def to_dict(self):
        """
        Returns a dict representation of this object
        """
        equipment_data = {}
        equipment_data['qa_equipment_schema_version'] = "1.0"
        equipment_data['clusters'] = []
        for cluster in self._cluster_list:
            equipment_data['clusters'].append(cluster.to_dict())
        equipment_data['clients'] = []
        for client in self._client_list:
            equipment_data['clients'].append(client.to_dict())
        return equipment_data

    ####################

    def get_cluster_list(self, required=True):
        """
        Returns a list of ClusterEquipment objects
          required (bool) - Whether to raise EquipmentNotFoundError
                            if none are found
        """
        if required and not self._cluster_list:
            raise EquipmentNotFoundError("No cluster found")
        # Defensive copy return
        return self._cluster_list[:]

    def get_cluster(self, required=True):
        """
        Returns a single ClusterEquipment object
          required (bool) - Whether to raise EquipmentNotFoundError
                            if none are found
        """
        cluster_list = self.get_cluster_list(required=required)
        if cluster_list:
            return cluster_list[0]
        elif not required:
            return None
        else:
            raise EquipmentNotFoundError("No cluster found")

####################

    def get_client_list(self, required=True, osname=None):
        """
        Returns a list of ClientEquipment objects
        Optional parameters:
          required (bool) - Whether to raise EquipmentNotFoundError if
                            none are found, otherwise return an empty list
          osname (str) - if None, return any clients, else only return clients
                     with the correct OS (e.g. "Linux")
        """
        if osname is None:
            client_list = list(self._client_list)
        else:
            client_list = [client for client in self._client_list
                           if client.get_os() == osname]
        if required and not client_list:
            raise EquipmentNotFoundError("No client found")
        return client_list

    def get_client(self, required=True, osname=None):
        """
        Returns a single ClientEquipment object
        Optional parameters:
          required (bool) - Whether to raise EquipmentNotFoundError
                            if none was found, otherwise returns None
          osname (str) - if None, return any client, else only return a client
                     with the correct OS (e.g. "Linux")
        """
        client_list = self.get_client_list(required=required, osname=osname)
        if client_list:
            # Return the last one created.  This allows us to override the
            # Database client with the qarunner `--client` option
            return client_list[-1]
        elif not required:
            return None
        else:
            raise EquipmentNotFoundError("No client found")

    ####################
    #
    # Methods to load equipment data into this EquipmentProvider

    def _load_from_json_description_file(self, filename):
        """
        This method should be treated as package-private.
        Loads an equipment definition from a JSON file
        Parameter:
          filename (str)

        JSON file example:
          {
              "qa_equipment_schema_version": "1.0",
              "description": "110s cluster",
              "cluster_defaults": {
                  "admin_username": "admin",
                  "admin_password": "my_admin_pass"
              },
              "client_defaults": {
                  "root_password": "my_root_pass"
              },
              "clusters": [
                  {
                      "cluster_type": "hardware",
                      "name": "110s",
                      "mgmt_vip_addr": "172.16.6.110",
                      "node_hostname_list": [
                          "xyz110.my.domain.com",
                          "xyz111.my.domain.com",
                          "xyz112.my.domain.com"
                      ]
                  }
              ],
              "clients": [
                  {
                      "hostname": "xyz65.my.domain.com"
                  }
              ]
          }
        """
        with open(filename, 'r') as fp:
            equipment_data = json.load(fp, encoding='utf-8')
        schema_version = \
            equipment_data.get("qa_equipment_schema_version", None)
        if not schema_version:
            raise ValueError("qa_equipment_schema_version must be defined")

        cluster_defaults = equipment_data.get("cluster_defaults", {})
        cluster_data_list = equipment_data.get("clusters", [])
        for cluster_data in cluster_data_list:
            # apply defaults:
            for key in cluster_defaults:
                if key not in cluster_data:
                    cluster_data[key] = cluster_defaults[key]
            cluster = clusterequipment.from_dict(
                          cluster_data,
                          qa_equipment_schema_version=schema_version)
            self._cluster_list.append(cluster)

        client_defaults = equipment_data.get("client_defaults", {})
        client_data_list = equipment_data.get("clients", [])
        for client_data in client_data_list:
            # apply defaults:
            for key in client_defaults:
                if key not in client_data:
                    client_data[key] = client_defaults[key]
            client = clientequipment.from_dict(
                          client_data,
                          qa_equipment_schema_version=schema_version)
            self._client_list.append(client)

    def _load_from_dict(self, equipment_data):
        """
        This method should be treated as package-private.
        Loads an equipment definition from a python dictionary
        Parameter:
          equipment_data (dict)

        Dict example:
          {
              "qa_equipment_schema_version": "1.0",
              "description": "110s cluster",
              "cluster_defaults": {
                  "admin_username": "admin",
                  "admin_password": "my_admin_pass",
              },
              "client_defaults": {
                  "root_password": "my_root_pass"
              },
              "clusters": [
                  {
                      "cluster_type": "hardware",
                      "name": "110s",
                      "mgmt_vip_addr": "172.16.6.110",
                      "node_hostname_list": [
                          "xyz110.my.domain.com",
                          "xyz111.my.domain.com",
                          "xyz112.my.domain.com"
                      ]
                  }
              ],
              "clients": [
                  {
                      "hostname": "xyz65.my.domain.com"
                  }
              ]
          }
        """
        schema_version = \
            equipment_data.get("qa_equipment_schema_version", None)
        if not schema_version:
            raise ValueError("qa_equipment_schema_version must be defined")

        cluster_defaults = equipment_data.get("cluster_defaults", {})
        cluster_data_list = equipment_data.get("clusters", [])
        for cluster_data in cluster_data_list:
            # apply defaults:
            for key in cluster_defaults:
                if key not in cluster_data:
                    cluster_data[key] = cluster_defaults[key]
            cluster = clusterequipment.from_dict(
                cluster_data,
                qa_equipment_schema_version=schema_version)
            self._cluster_list.append(cluster)

        client_defaults = equipment_data.get("client_defaults", {})
        client_data_list = equipment_data.get("clients", [])
        for client_data in client_data_list:
            # apply defaults:
            for key in client_defaults:
                if key not in client_data:
                    client_data[key] = client_defaults[key]
            client = clientequipment.from_dict(
                client_data,
                qa_equipment_schema_version=schema_version)
            self._client_list.append(client)

    # TODO: rename this to something more precise
    def _load_from_cluster_mgmt_hostname_list(self,
                                              mgmt_hostname_list,
                                              admin_username=None,
                                              admin_password=None,
                                              mgmt_vip_addr=None):
        """
        This method should be treated as package-private.

        Loads a cluster into this EquipmentProvider based on a list of
        cluster management hostnames/IPs.

        Parameters:
          mgmt_hostname_list (list) - One IP per server node in the cluster
          admin_username (str) - e.g. "admin"
          admin_password (str) - e.g. "my_admin_pass"
          mgmt_vip_addr (str) - e.g. "172.16.2.119"
        """
        get_cluster = clusterequipment.from_server_hostname_list
        cluster = get_cluster(mgmt_hostname_list,
                              admin_username=admin_username,
                              admin_password=admin_password,
                              mgmt_vip_addr=mgmt_vip_addr)
        self._cluster_list.append(cluster)     # TODO: check for dups

    def _load_from_client_hostname(self, client_hostname,
                                   username=None, password=None,
                                   name=None):
        """
        This method should be treated as package-private.

        Loads a client into this EquipmentProvider based on the client
        hostname/IP.

        Parameter:
          client_hostname (str)
          username (str) - e.g. "root"
          password (str) - e.g. "my_root_pass"
          name (str) - e.g. "node4"
        """
        client = clientequipment.from_hostname(client_hostname,
                                               username=username,
                                               password=password,
                                               name=name)
        self._client_list.append(client)    # TODO: check for dups

    def _load_from_cluster_Ip(self, hostname):
        """
        Loads a cluster into this EquipmentProvider based on a single
        IP.

        Parameters:
          hostname - One IP for a node in cluster
        """
        self._load_from_cluster_mgmt_hostname_list([hostname])

    def _load_from_str(self, equipment_label):
        """
        Populates this EquipmentProvider based on the string which
        identifies the cluster (the qarunner.py -c argument)
        """
        if type(equipment_label) == dict:
            self._load_from_dict(equipment_label)
        elif equipment_label.lower().endswith(".json"):
            self._load_from_json_description_file(equipment_label)
        else:
            raise ValueError("Cannot create EquipmentProvider from {}".format(
                equipment_label))


def from_str(equipment_label):
    """
    Returns an EquipmentProvider object, defined by a string (suitable to
    be passed to qarunner.py -c <str>)
    """
    instance = EquipmentProvider()
    instance._load_from_str(equipment_label)
    return instance

###############################################################################


_default_equipment_provider = None


def get_default_equipment_provider():
    """
    Returns the default EquipmentProvider object configured for this package
    Raises EquipmentNotFoundError if no default has been set.
    """
    if _default_equipment_provider is None:
        raise EquipmentNotFoundError("No equipment configured")
    return _default_equipment_provider


def set_default_equipment_provider(equipment_provider):
    """
    Sets the default EquipmentProvider object configured for this package
    """
    global _default_equipment_provider
    _default_equipment_provider = equipment_provider
