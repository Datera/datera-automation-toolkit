# -*- coding: utf-8 -*-
"""
This module provides the Client class
"""
from __future__ import (print_function, unicode_literals, division)


import qalib.corelibs
import qalib.corelibs.credentials

__copyright__ = "Copyright 2020, Datera, Inc."


class ClientEquipment(object):

    """
    Client equipment specification.

    Tests and tools should not interact with this directly.
    This is used by the qalib.client package.
    """

    def __init__(self, hostname, root_creds, name=None):
        self._hostname = hostname
        self._root_creds = root_creds
        if name:
            self.name = name
        else:
            self.name = hostname

    def to_dict(self):
        client_data = {}
        client_data["hostname"] = self._hostname
        client_data["root_password"] = self._root_creds.get_password()
        return client_data

    def get_credentials(self):
        """
        Returns a Credentials object to be used for root access to the client
        """
        return self._root_creds

    def get_os(self):
        """
        Returns the OS type
        """
        return "Linux"  # TODO

    def get_connection(self):
        """
        Returns a SystemConnection to communicate with this client
        """
        creds = self._root_creds
        return qalib.corelibs.systemconnection.from_hostname(
            self._hostname, creds=creds, name=self.name)


def from_hostname(hostname, username=None, password=None, name=None):
    """
    Creates a ClientEquipment instance from its hostname/IP
    Parameter:
      hostname (str) - IP or resolveable hostname
    Optional parameters:
      username (str) - e.g. "root"
      password (str) - e.g. "my_root_pass"
      name (str) - e.g. "node4"
    This is package-private.
    """
    if username is None and password is None:
        root_creds = \
            qalib.corelibs.credentials.get_default_client_root_credentials()
    else:
        root_creds = qalib.corelibs.credentials.from_user_pass(
            username, password)
    if not name:
        name = hostname
    instance = ClientEquipment(hostname, root_creds=root_creds, name=name)
    return instance


def from_dict(client_data, qa_equipment_schema_version=None):
    """
    Returns a ClientEquipment instance from a dictionary, which is usually
    a subsection of a JSON config file.

    Example:
        {"hostname": "rts65.daterainc.com", "root_password": "my_pass"}
    """
    if str(qa_equipment_schema_version) != "1.0":
        raise NotImplementedError("Unknown schema version: %r" %
                                  qa_equipment_schema_version)
    hostname = client_data.get("hostname", None)
    if not hostname:
        raise ValueError("Client hostname must be specified")
    root_creds = None
    root_username = client_data.get("root_username", "root")
    root_password = client_data.get("root_password", None)
    if root_username and root_password:
        root_creds = qalib.corelibs.credentials.from_user_pass(root_username,
                                                               root_password)
    return ClientEquipment(hostname, root_creds=root_creds)
