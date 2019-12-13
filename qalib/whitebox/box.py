# -*- coding: utf-8 -*-
"""
Provides the Whitebox object
"""
from __future__ import (unicode_literals, print_function, division)

import logging

import qalib.corelibs
import qalib.corelibs.systemconnection
import qalib.api
import qalib.qabase.parsers

__copyright__ = "Copyright 2020, Datera, Inc."

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class Whitebox(qalib.corelibs.system.System):
    """
    Controls a remote server node.

    The on_hardware attribute is True if this server is on hardware, False
    if it is a VM, and None if unknown.
    """

    def __init__(self, *args, **kwargs):
        """
        Call super-class __init__, set attributes
        """
        super(Whitebox, self).__init__(*args, **kwargs)
        self.on_hardware = None
        self._cli = None

    @property
    def cli(self):
        """ The Datera admin CLI """
        if not self._cli:
            from qalib.whitebox.datera_cli import DateraCli
            creds = self._system_conn._creds
            self._cli = DateraCli(self.ip, creds.get_username(),
                                  creds.get_password())
        return self._cli

    @classmethod
    def from_node(cls, server):
        """
        Parameter:
          server (qalib.api.ApiResource)
        """
        api = qalib.api.from_entity(server)
        clusterequipment = qalib.api.get_clusterequipment(api)
        creds = clusterequipment.get_admin_creds()

        server_name = server.get('name', '')
        # If server hostname is not resolvable or assigned, we need to work
        # with ip address. If hostname is available, let's just use that.
        if not server_name or "localhost" in server_name:
            server_ip = clusterequipment.util.get_server_mgmt_ip_address(
                server)
            if not server_name:
                server_name = server_ip
            sysconn = qalib.corelibs.systemconnection.from_hostname(
                server_ip, creds=creds, name=server_name)
        else:
            server_name = server['name']
            sysconn = clusterequipment.get_server_systemconnection(server_name)
        instance = cls.from_connection(sysconn)

        # e.g. "node1" instead of "172.30.103.2"
        instance.name = server_name

        # Set a flag indicating whether this is an actest or hardware server:
        if clusterequipment.on_hardware:
            instance.on_hardware = True
        else:
            instance.on_hardware = False
        return instance

    def time(self):
        """
        Returns the system time as in int, in UNIX Epoch format,
        similar to what time.time() returns.

        Normally, you should use clusterutil.time(), instead.
        """
        out = self.run_cmd_check("date +%s")
        try:
            return int(out)
        except ValueError:
            raise EnvironmentError("date command failed: " + repr(out))
