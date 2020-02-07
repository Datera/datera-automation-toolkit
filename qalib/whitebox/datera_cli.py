# -*- coding: utf-8 -*-
"""
Provides the object to work with DateraCLI
"""
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
import logging
import re

from qalib.corelibs.ssh import InteractiveSSH


__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())


class DateraCli(object):

    # Different modes
    LOCAL_MODE = "local"
    CLUSTER_MODE = "cluster"
    ANY_MODE = "any"

    # Prompts for different modes
    REGEX_PROMPT = {LOCAL_MODE: "Local(?:@\w+)? >>",
                    CLUSTER_MODE: "Datera .*>>",
                    ANY_MODE: "Local(?:@\w+)? >>|Datera .*>>"}

    def __init__(self, hostname, username="admin", password=None, port=22):
        self._hostname = hostname
        self._username = username
        self._password = password
        self._port = port
        self._use_scriptmode = False
        self.__ishell = None

    @property
    def _ishell(self):
        if self.__ishell and not self.__ishell.connected:
            self.__ishell = None
        if not self.__ishell:
            self.__ishell = InteractiveSSH(self._hostname, self._username,
                                           self._password, self._port)
            # node will always start with local mode
            self.__ishell.connect(self.REGEX_PROMPT[self.LOCAL_MODE])
            if self._use_scriptmode:
                output = self.__ishell.send_command(
                    "scriptmode", self.REGEX_PROMPT[self.LOCAL_MODE])
                # if "Script mode is now ON" not in output:
                #    msg = ("Failed to configure Cli into scriptmode: %s"
                #           % (output))
                #    raise CliError(msg)
        return self.__ishell

    @_ishell.setter
    def _ishell(self, value):
        self.__ishell = value

    def close(self):
        if self.__ishell is None:
            return
        self._ishell.close()
        self._ishell = None

    def _login(self):
        # send login command, expect username prompt
        self._ishell.send_command("login", "Username:")
        # send username, expect password prompt
        self._ishell.send_command(self._username, "Password:")
        # send password
        self._ishell.send_command(self._password,
                                  self.REGEX_PROMPT[self.CLUSTER_MODE])

    # @_check_connection
    def get_current_mode(self):
        output = self._ishell.send_command(
            cmd="x", prompt_regex=self.REGEX_PROMPT[self.ANY_MODE],
            nostrip=True)
        lines = output.splitlines()
        if re.search(self.REGEX_PROMPT[self.LOCAL_MODE], lines[-1]):
            return self.LOCAL_MODE
        elif re.search(self.REGEX_PROMPT[self.CLUSTER_MODE], lines[-1]):
            return self.CLUSTER_MODE
        else:
            raise CliError("CLI is in unknown state: %s" % output)

    def _enter_local_mode(self):
        """
        Enters the local mode
        """
        current_mode = self.get_current_mode()
        if current_mode == self.LOCAL_MODE:
            log.debug("Already in local mode")
            return
        elif current_mode == self.CLUSTER_MODE:
            log.debug("In cluster mode, going to local")
            self._ishell.send_command("local",
                                      self.REGEX_PROMPT[self.LOCAL_MODE])
            return
        else:
            # we should not reach here!
            raise CliError("CLI is in unknown state: %s" % current_mode)

    def _enter_cluster_mode(self):
        """
        Enters the cluster mode
        """
        current_mode = self.get_current_mode()
        if current_mode == self.CLUSTER_MODE:
            log.debug("Already in cluster mode")
            return
        elif current_mode == self.LOCAL_MODE:
            # enter cluster mode
            log.debug("In local mode, going to cluster")
            self._ishell.send_command("cluster",
                                      self.REGEX_PROMPT[self.CLUSTER_MODE])
            # need to login
            self._login()
            log.debug("Successfully logged into cluster mode")
            return
        else:
            # we should not reach here!
            raise CliError("CLI is in unknown state: %s" % current_mode)

    def _enter_mode(self, mode):
        """
        Enters the mode requested. "Cluster" or "Local".
        """
        if mode == self.LOCAL_MODE:
            return self._enter_local_mode()
        elif mode == self.CLUSTER_MODE:
            return self._enter_cluster_mode()
        else:
            raise CliError("Unknown mode requested:%s" % mode)
    # @_check_connection
    def exec_command(self, cmd, mode="local", timeout=60):
        """
        Execute command on datera cli. Returns the output.

        Parameters:
           cmd (str) - command to be executed
           mode (str) - mode to execute the command
           timeout (int) - command runtime max timeout in secs
        """
        # TODO- add error handling for datera cli
        # enter the correct mode
        self._enter_mode(mode)
        prompt_regex = self.REGEX_PROMPT[mode]
        output = self._ishell.send_command(cmd, prompt_regex,
                                           timeout=timeout)
        return output
