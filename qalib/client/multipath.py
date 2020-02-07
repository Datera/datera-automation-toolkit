# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

__copyright__ = "Copyright 2020, Datera, Inc."

class Multipath(object):
    """
    This method enables the multipath on the client to be used
    """

    def __init__(self, system, os_version=None):
        self.system = system
        self.multipath_objects = []
        self.client_type = None
        if os_version is None:
            msg = "Please provide OS version to multipath object"
            raise ValueError(msg)
        self.os_version = os_version.lower()

    def enable_multipath(self):
        """
        Installs the multipath libraries  and copies the configuration.
        """
        def _install_multipath():
            """
            Installs the multipath libraries.
            """
            if self.is_centos():
                cmd = "yum install device-mapper-multipath.x86_64 -y"
                ret, data = self.system.run_cmd(cmd)
                self.client_type = 'rhel'
                if ret:
                    msg = ('Failed to install the Multipath package on {},'
                           ' response from install:\n{}'.format(
                            self.system.name, data))
                    raise Exception(msg)
            else:
                 raise RuntimeError("Clients must be CentOS systems.")
