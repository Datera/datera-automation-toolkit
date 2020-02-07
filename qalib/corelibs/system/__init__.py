# -*- coding: utf-8 -*-
"""
Provides the System object
"""
__copyright__ = "Copyright 2020, Datera, Inc."

import logging
import multiprocessing
import uuid
import pipes

from qalib.corelibs.system.networking import ping, Networking
from qalib.corelibs.system.filesystem import FileSystem
from qalib.equipment.os_util_parsers import get_util_parser
import qalib.corelibs.credentials

# dictionary for limiting concurrency on client level udevadm commands.
# These commands are single threaded but have timeouts based on when they are
# issued.  For example, if you issue 5 commands at the same time with a 500
# second timeout.  Only one will be processed at a time, so if each command
# only takes 105 seconds the 6th command will timeout.
_UDEV_SEMIS = {}

# This timeout is relative to how many volumes are mounted unmounted.
# With more volumes comes a longer timeout.
_UDEV_TIMEOUT = 960

# dictionary for limiting concurrency on client level iscsiadm commands.
_ISCSIADM_SEMIS = {}

# lock that prevents editing the data structures in parallel
_SEMI_LOCKS = multiprocessing.Lock()

# Global Dictionary for storing the logged in targets
ISCSI_LOGGEDIN_TARGETS = {}

# Global Dictionary for storing discovered clients
DISCOVERED_TARGETS = {}


class System(object):

    """
    Controls a remote system
    This is the base class for Client and Whitebox objects
    For now, only Linux systems are supported.

    Attributes:
      name - A hostname or node name.  Human-readable, for logging purposes.
             Not guaranteed to be resolveable or to match `hostname`.
      ip - An IP which automation can use to access the system
    """

    def __init__(self, systemconnection, logger=None):
        """ Do not instantiate this directly; use a factory function """
        self.os = "Linux"   # platform.system() / uname -s
        self._system_conn = systemconnection
        if logger is None:
            logger = logging.getLogger(__name__)
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())
        self._logger = logger
        self.name = systemconnection.name
        self.ip = systemconnection.ip
        self._networking = None
        self._filesystem = None
        self._util = None
        with _SEMI_LOCKS:
            if self.name not in _UDEV_SEMIS:
                _UDEV_SEMIS[self.name] = multiprocessing.Semaphore(1)
            if self.name not in _ISCSIADM_SEMIS:
                _ISCSIADM_SEMIS[self.name] = multiprocessing.Semaphore(8)

    def __repr__(self):
        return ('<%s.%s name=%r ip=%r>' %
                (self.__module__, type(self).__name__, self.name, self.ip))

    @classmethod
    def from_connection(cls, systemconnection):
        """
        Returns an instance to communicate with a remote system
        Parameter:
          systemconnection (qalib.corelibs.systemconnection.SystemConnection)
        """
        return cls(systemconnection)

    @classmethod
    def from_hostname(cls, hostname, username, password):
        """
        Returns an instance to communicate with a remote system
        Parameters:
          hostname (str) - network name or IP address
          username (str) - e.g. "root"
          password (str)
        """
        creds = qalib.corelibs.credentials.from_user_pass(username, password)
        conn = qalib.corelibs.systemconnection.from_hostname(hostname, creds)
        return cls.from_connection(conn)

    @property
    def util(self):
        if not self._util:
            self._util = get_util_parser(self)
        return self._util

    @property
    def networking(self):
        """
        Returns networking object for systems
        """
        if not self._networking:
            self._networking = Networking(self)
        return self._networking

    @property
    def filesystem(self):
        """
        Returns filesystem object for systems
        """
        if not self._filesystem:
            self._filesystem = FileSystem(self)
        return self._filesystem

    def run_cmd(self, cmd):
        """
        Runs a command, returns a (exit_status, output) tuple
        """
        if isinstance(cmd, (list, tuple)):
            cmd = " ".join(cmd)
        exitstatus, output = self._system_conn.run_cmd(cmd)
        return exitstatus, output

    def run_cmd_check(self, cmd):
        """
        Runs a command, returns its output
        Raises an EnvironmentError if the command exits non-zero
        """
        exitstatus, output = self.run_cmd(cmd)
        if exitstatus != 0:
            raise EnvironmentError("Command failed ($?={}) on {}:\n{}\n{}"
                                   .format(exitstatus, self.name, cmd, output))
        return output

    def run_async_cmd(self, cmd):
        """
        Runs a command in non blocking mode. returns an async object

        NOTE: this should be used only part of with statement as this
        is a context manager

        Eg.
        with client.run_async_cmd(cmd) as fio:
            # bring down node
            # validate output
            assert "error=0" in fio.read()
            # wait for command to finish
            status = fio.wait()
            assert status == 0

        :returns: Async object of cmd executed
        """
        if isinstance(cmd, (list, tuple)):
            cmd = " ".join(cmd)
        return self._system_conn.run_async_cmd(cmd)

    def path_exists(self, filepath):
        """
        Returns a bool, True if the remote path exists, else False.
        The reason we are using an "ls" instead of test, is to prevent false
        positives.  test -e will follow the path for broken symlinks.
        """
        exitstatus, _output = self.run_cmd("ls " + pipes.quote(filepath))
        if exitstatus == 0:
            return True
        else:
            return False

    def makedirs(self, path):
        """
        Creates a dirctory with path provided
        """
        self.run_cmd_check("mkdir -p " + pipes.quote(path))

    def path_isfile(self, filepath):
        """
        Returns a bool, True if the remote path is a regular file, else False
        """
        exitstatus, _output = self.run_cmd("test -f " + pipes.quote(filepath))
        if exitstatus == 0:
            return True
        else:
            return False

    def file_open(self, filepath, mode):
        """
        Returns a file-like object which can do I/O to a remote file.
        Parameters:
          filepath (str) - The remote file path
          mode (str) - e.g. 'r', 'w', 'a'
        """
        return self._system_conn.file_open(filepath, mode)

    def file_put(self, localpath, remotepath):
        """
        Copies a file to the remote system
        Parameters:
          localpath (str) - The local file path
          remote (str) - The remote file path
        """
        return self._system_conn.file_put(localpath, remotepath)

    def file_get(self, remotepath, localpath):
        """
        Copies a file from the remote system
        Parameters:
          remote (str) - The remote file path
          localpath (str) - The local file path
        """
        return self._system_conn.file_get(remotepath, localpath)

    def tcp_open(self, hostname, port):
        """
        Method sets up a direct-tcp channel to allow client to act as a
        gateway or proxy. Eg. usecases is for making qalib.api calls from
        client and not the system test is being run from

        Parameters:
          hostname (str) - host to connect to. Eg. 172.28.119.9
          port (int) - port to connect to. Eg. 7717
        Returns:
          A socket-like object
        """
        return self._system_conn.tcp_open(hostname, port)

    def is_pingable(self):
        '''
        return true if the wb is pingable
        else false
        '''
        return ping(self.ip, partial=True,
            timeout=1, count=1)

    def listdir(self, path):
        """
        List all the dir for the given path
        """
        cmd = "ls -- " + pipes.quote(path)
        ret, proc = self.run_cmd(cmd)
        if not ret:
            files = proc.split("\n")
            return [x for x in files if x != ""]
        else:
            raise EnvironmentError("List Dir returned an error")

    def mkdtemp(self):
        """
        Creates a temporary directory on the remote system.
        Returns the directory path.
        The caller is responsible for cleaning it up.
        """
        random_str = str(uuid.uuid1()).replace('-', '')
        tmpdir = "/tmp/tmpdir." + random_str
        self.run_cmd_check("mkdir " + tmpdir)
        return tmpdir

    def remove_filepath(self, filepath):
        """
        Remove a file path on the remote system, using "rm -rf".
          filepath (str) - File or directory path
        It is OK if the filepath does not exist.
        """
        self.run_cmd_check("rm -rf -- " + pipes.quote(filepath))
