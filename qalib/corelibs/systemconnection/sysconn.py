# -*- coding: utf-8 -*-
"""
Provides the SystemConnection class and factory function
"""
from __future__ import (unicode_literals, print_function, division)

import logging

from qalib.qabase.exceptions import with_exception_translation
from qalib.qabase.networking import gethostbyname_retry
# from . import corelibs_bridge
from ..ssh import SSH

__copyright__ = "Copyright 2020, Datera, Inc."


def _to_str(u):
    try:
        return str(u)
    except UnicodeEncodeError:
        return repr(u)[1:-1]


class SystemConnection(object):
    """
    This controls a remote system via SSH

    Tests and tools should not use this object directly.
    """

    def __init__(self, hostname, creds, name=None, logger=None):
        """
        Do not instaniate directly; use from_hostname()
        Parameters:
          hostname (str) - an IP or resolveable hostname
          creds (qalib.corelibs.credentials.Credentials)
        Optional parameters:
          name (str) - Human-readable name, not necessarily resolveable
                       In the case of server nodes, this will be the node
                       name.
          logger (logging.Logger)
        """
        if logger is None:
            logger = logging.getLogger(__name__)
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())
        self._logger = logger
        # work around product bug:
        if hostname.endswith(".(none)"):
            self._logger.warn("Correcting invalid hostname: " + hostname)
            hostname = hostname[:-7]
        self._hostname = hostname
        self._creds = creds

        if name:
            self.name = name
        else:
            self.name = hostname

        self.ip = gethostbyname_retry(hostname)

    def get_creds(self):
        """ Returns a Credentials object for logging into the system """
        return self._creds

    def _get_shell(self):
        """ Returns an SSH object which utilizes paramiko.SSHClient """
        port = 22
        shell = SSH(self.ip,
                    self._creds.get_username(),
                    self._creds.get_password(),
                    port)
        # self._check_connection(shell)
        return shell

    @staticmethod
    def _check_connection(shell):
        """
        Ensures that the shell object is populated and a non empty return
        value is received from a test command.
        """
        if not shell:
            raise EnvironmentError("No shell connection dectected")
        else:
            check = shell.exec_command("echo thisworks")
            if check == "":
                raise EnvironmentError("Shell is not working correctly")

    @with_exception_translation
    def run_cmd(self, cmd, error_expected=True):
        """
        Executes a command on the remote server
        Returns an (exitstatus, output) tuple
        """
        shell = self._get_shell()
        self._logger.debug("Execute command on " + str(self._hostname) +
                           ": " + _to_str(cmd))
        exit_status, output = shell.exec_command(cmd,
                                                 error_expected=error_expected)
        self._logger.debug("Ran command on " + str(self._hostname) + ":\n" +
                           _to_str(cmd) + "\n" +
                           "$? = " + str(exit_status) + "\n" +
                           _to_str(output))

        return exit_status, output

    @with_exception_translation
    def run_async_cmd(self, cmd):
        """
        Executes a command asyncronous / non blocking on remote server

        NOTE: This is a context manager and should be used in a 'with'
        statement.

        Eg.

        configure_setup()
        with run_async_cmd("tail -f /var/log/syslog) as f:
            assert "ERROR" not in f.read()
            # do other test stuff
            assert "ERROR" not in f.read()
        # do some more test stuff if needed
        deconfigure_setup()

        :returns: Async object for the command being run
        """
        self._logger.debug("Start async command on " + str(self._hostname) +
                           ": " + _to_str(cmd))
        ssh_conn = self._get_shell()
        return ssh_conn.exec_command_async(cmd)

    @with_exception_translation
    def file_open(self, filepath, mode):
        """
        Returns a file-like object which can do I/O to a remote file.
        Parameters:
          filepath (str) - The remote file path
          mode (str) - e.g. 'r', 'w', 'a'
        """
        ssh_conn = self._get_shell()
        return ssh_conn.file_open(filepath, mode)

    @with_exception_translation
    def file_put(self, localpath, remotepath):
        """
        Copies a file to the client system
        Parameters:
          localpath (str) - The local file path
          remote (str) - The remote file path
        """
        with open(localpath, 'r') as localfile:
            with self.file_open(remotepath, 'w') as remotefile:
                while True:
                    data = localfile.read(4096)
                    if not data:
                        break
                    remotefile.write(data)

    @with_exception_translation
    def file_get(self, remotepath, localpath):
        """
        Copies a file from the client system
        Parameters:
          remote (str) - The remote file path
          localpath (str) - The local file path
        """
        with open(localpath, 'w') as localfile:
            with self.file_open(remotepath, 'r') as remotefile:
                while True:
                    data = remotefile.read(65536)
                    if not data:
                        break
                    localfile.write(data)

    @with_exception_translation
    def tcp_open(self, remote_host, remote_port):
        """
        Note: This should not be called directly.  Use
        qalib.corelibs.system.tcp_open instead!

        Method sets up a direct-tcp channel to allow client to act as a
        gateway or proxy. Eg. usecases is for making qalib.api calls from
        client and not the system test is being run from

        Parameters:
          remote_host (str) - remote host to connect to. Eg. 172.28.119.9
          remote_port (int) - remote port to connect to. Eg. 7717
        Returns:
          A socket-like object
        """
        ssh = self._get_shell()
        return ssh.tcp_open(remote_host, remote_port)


def from_hostname(hostname, creds=None, name=None):
    """
    Parameters:
      hostname (str) - IP or resolveable hostname
      creds (qalib.corelibs.credentials.Credentials) - root login credentials
    Optional parameters:
      name (str) - Human-readable name, not necessarily resolveable
                   In the case of server nodes, this will be the node
                   name.
    """
    if creds is None:
        raise ValueError("Credentials missing!")
    return SystemConnection(hostname, creds, name=name)
