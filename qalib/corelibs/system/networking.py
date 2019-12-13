# -*- coding: utf-8 -*-
from __future__ import (unicode_literals, print_function, division)
"""
Provides the Networking tool
"""

import logging
import subprocess

__copyright__ = "Copyright 2020, Datera, Inc."

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class Networking(object):
    """
    Provides networking related libraries on a system
    """

    def __init__(self, system):
        """
        Creates a Networking object for system type
        !! Should not be called directly !
        This should be accessible via systems.networking
        property created
        """
        self.system = system

    def ping(self, target, count=5, interval=None, size=None, partial=False,
             timeout=None):
        """
        Runs ping connectivity test to a end-point
        Parameter:
          target (str) - Remote end-point, can be hostname or ip address
          count (int) - Number of pings, defaults to 5
          interval (float) - Time interval in seconds between each ping request
          size (int) - Packet size for ping test
          partial (bool) - Returns pass even if there is a partial success
          timeout (int) - time in sec to wait for response
        Returns True or False for pass/fail
        Raises EnvironmentError on failure to run ping
        """
        return ping(target, count=count, interval=interval, size=size,
                    partial=partial, system=self.system, timeout=timeout)


def ping(target, count=5, interval=None, size=None, partial=False,
         system=None, timeout=None):
    """
    Runs ping connectivity test to a end-point
    Parameter:
      target (str) - Remote end-point, can be hostname or ip address
      count (int) - Number of pings, defaults to 5
      interval (float) - Time interval in seconds between each ping request
      size (int) - Packet size for ping test
      partial (bool) - Returns pass even if there is a partial success
      system (System object) - System that this ping command should run on.
                               Runs locally if not provided
      timeout (int) - time in sec to wait for response
    Returns True or False for pass/fail
    Raises EnvironmentError on failure to run ping
    """

    args = ["ping", "-n", "-c", unicode(count)]

    if not count:
        count = 1
    if interval:
        if float(interval) < 0.2:
            msg = "Min interval for ping is 200mS"
            raise ValueError(msg)
        args.extend(["-i", unicode(interval)])
    if size:
        args.extend(["-s", unicode(size)])
    if timeout:
        args.extend(["-W", unicode(timeout)])

    args.append(unicode(target))

    if system:
        exitstatus, output = system.run_cmd(" ".join(args))
    else:
        exitstatus = 2
        output = None
        p = subprocess.Popen(args, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        output = p.communicate()[0]
        exitstatus = p.returncode
        logger.debug("Ran command on localhost:\n" +
                     " ".join(args) + "\n" +
                     "$? = " + str(exitstatus) + "\n" + str(output))

    lines = output.splitlines()

    # exit status 2 implies wrong command / invalid hostname
    if exitstatus != 2:
        result_lines = [line for line in lines
                        if "packets transmitted" in line]
        if len(result_lines) != 1:
            msg = "Cannot parse ping output: %s" % output
            raise EnvironmentError(msg)
        result_line = result_lines[0]

        loss_lines = [x for x in result_line.split(', ') if "loss" in x]
        if len(loss_lines) != 1:
            msg = "Cannot parse ping output: %s" % output
            raise EnvironmentError(msg)
        loss_line = loss_lines[0]
        loss = int(float(loss_line.split("%")[0]))

        # If partial flag is enabled, we need to return True for all
        # values with less than 100% failure / packet loss
        # If partial flag is disabled, only 0 packet drops are acceptable
        if (loss != 100 and partial) or loss == 0:
            return True
        else:
            return False
    else:
        msg = "The ping command returned the following output: %s" % output
        raise EnvironmentError(msg)
