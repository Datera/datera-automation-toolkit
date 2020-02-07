# -*- coding: utf-8 -*-
"""
Provides the Base IO object
"""
from __future__ import (unicode_literals, print_function, division)

import logging
import threading
import uuid

__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())


class IoBase(object):
    """
    This is the base class for all IO load generator objects
    """

    # Sub-classes should over-ride this, but they should call up into
    # this super-class method, and pass along all their keyword arguments
    def __init__(self, iospec, **kwargs):
        """ Do not instantiate this directly; use a factory function """
        # track internal state (e.g. to prevent calling start() twice):
        self.__lock = threading.Lock()
        self.__io_has_been_started = False
        self.__io_has_been_stopped = False
        self.__output_file_has_been_created = False
        self.__orig_kwargs = kwargs.copy()
        self.__async_shell = None
        self.__final_output = ''
        # These variables are shared with sub-classes:
        self._iospec = iospec
        self._ioparams = kwargs
        self._client = self._iospec.get_client()
        self._output_file = None
        self._execute_cmd = None  # sub-classes will set this in start()
        self._id = str(uuid.uuid1()).replace('-', '')  # random unique str
        self.cleanup = True

    def _setup_output_file(self):
        """ Call this before starting fio to set self._output_file """
        # Prevent calling this twice:
        if self.__output_file_has_been_created:
            raise ValueError("I/O object seems to have been reused")
        self.__output_file_has_been_created = True
        with self.__lock:
            if self._output_file:
                return
            self._output_file = "/var/tmp/IO." + self._id + ".out"
            self._client.run_cmd("touch " + self._output_file)

    def _cleanup_output_file(self):
        """ Called after IO has completed; deletes self._output_file """
        if self._output_file is None or self._client is None:
            return
        with self.__lock:
            self._client.run_cmd("rm -f -- " + self._output_file)
            self._output_file = None

    def get_exitstatus(self):
        """
        Returns an int, the exit code of the IO generator command.
        Returns None if exit code is not available (still running, not
        started yet, or not applicable for this tool).
        """
        if self.__async_shell is None:
            return None
        return self.__async_shell.exitstatus

    # If sub-classes over-ride this, they should call up into this
    # super-class method.
    def stop(self):
        """
        Stops an IO instance previously started with start().
        Normally, using this is not recommended; it is better to use
        start() with a context manager, which will call this automatically
        on leaving the "with" block.
        """
        if self.__async_shell is None:
            return  # already stopped
        self.__final_output = self.__async_shell.output
        self.__async_shell.kill()
        self.__async_shell = None
        self.__io_has_been_stopped = True

    def wait(self):
        """
        Wait for IO to finish. This is to be used in scenarios where we want
        IO to complete its run before calling the exit block

        :raises IOError: if IO exits with non 0 status
        """
        if self.__async_shell is None:
            return

        exitstatus = self.__async_shell.wait()
        self.__final_output = self.__async_shell.output
        self.__async_shell = None
        if exitstatus:
            raise IOError("IO failed on %s with: %s" % (
                self._client.name, exitstatus))

    # If sub-classes over-ride this, they should call up into this
    # super-class method.
    def start(self):
        """
        Start I/O traffic.
        Can be used as a context manager.

        Example:
             io = qalib.load.from_client_and_volume(self.client,
                                                    self.volume)
             with io.start():
                  # perform test steps
        """
        if self.__io_has_been_started:
            raise ValueError("start() cannot be called more than once")
        self.__io_has_been_started = True
        if not self._execute_cmd:
            raise ValueError("IO command is None "
                             "Fail to execute IO!!")
        if not self._client:
            raise ValueError("Client is not configured "
                             "Fail to execute IO!!")
        log.info("%s: %s", self._client.name, self._execute_cmd)
        shell = self._client.run_async_cmd(self._execute_cmd)
        if shell.exitstatus:
            raise IOError(
                "IO failed to start with exitcode:%s output:%s" % (
                    shell.exitstatus, shell.output))
        self.__async_shell = shell
        return self

    # Sub-classes should over-ride this
    def check_for_errors(self):
        """
        Raises an exception if any IO error has been encountered
        Example:
             with io.start():
                 ... long-running test steps ...
                 io.check_for_errors()
                 ... long-running test steps ...
        """
        # Default implementation which just checks for non-zero exit value
        exitstatus = self.get_exitstatus()
        if exitstatus is not None and exitstatus != 0:
            msg = ("%s: exited with return code %d" % (str(self), exitstatus))
            raise EnvironmentError(msg)
        return

    # Sub-classes should over-ride this
    def is_io_running(self, io_mode=None):
        """
        Method to see if I/O is running.
        Parameters:
          io_mode: list of stats to read. Can be "read", "write" or both
                   ["read"] - Returns True if it's reading
                   ["write"] - Returns True if it's writing
                   ["read", "write"] - Returns True if it's reading AND writing
                   [] - Returns True if it's reading OR writing (default)
        Returns: True/False for IO running/stopped
        """
        if not self.__io_has_been_started:
            return False  # not started yet
        if self.__io_has_been_stopped:
            return False  # stopped
        if self.get_exitstatus() is not None:
            return False  # exited
        return True
