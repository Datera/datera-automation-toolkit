# -*- coding: utf-8 -*-
"""
Provides the TestCase object
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import unittest
import logging
import os
import socket
import time

import qalib.equipment
import qalib.whitebox
from qalib.qabase.threading import Parallel


class TestCase(unittest.TestCase):
    """
    A base class for test cases.

    You can over-ride setUp() and tearDown().
    You could also over-ride setUpClass() and tearDownClass().

    For your test code, create a method whose name starts with "test",
    such as "test_volume_create", "test_run", etc.

    You should not over-ride any other methods or attributes from the
    TestCase super-class.

    Important attributes:
      self.logger (logging.Logger) - Use this for all messages
      self.equipment (qalib.corelibs.equipmentprovider.EquipmentProvider) -
          Pass this to any libraries which need to use test equipment.
    """
    _multiprocess_can_split_ = True
    _qatest_ = True

    ui = "REST"

    def __get_scriptname(self):
        """ Determines the basename of self's script, without .py/.pyc """
        mod = self.__module__
        try:
            if self.__module__ == "__main__":
                import __main__

                mod = os.path.splitext(os.path.basename(__main__.__file__))[0]
        except (ImportError, AttributeError):
            pass
        return mod.split('.')[-1]

    def id(self):
        """
        Over-rides the default id() function to change "__main__" to the
        actual script name, so that test results are reported consistently
        regardless of whether run by a runner or stand-alone.
        Also adds "_CLI" to the id if self.ui=="CLI"
        """
        testid = super(TestCase, self).id()
        if '__main__' in testid:
            testid = testid.replace('__main__', self.__get_scriptname())
        if hasattr(self, 'ui') and self.ui and self.ui != "REST":
            testid += "_" + str(self.ui)
        return testid

    def __init__(self, *args, **kwargs):
        """
        Call super-class __init__(), initialize custom attributes
        """
        super(TestCase, self).__init__(*args, **kwargs)
        self.equipment = qalib.equipment.get_default_equipment_provider()
        self.cluster = self.equipment.get_cluster(required=False)
        # self.logger = logging.getLogger(self.__module__ +
        #                                "." + self.__class__.__name__)
        self.logger = logging.getLogger(self.__get_scriptname())
        self._config_file = None  # lazy-loaded from property
        # TODO figure out how to only log this once.
        self._log_time()

    def _log_time(self):
        """
        As a means of keep time straight we want to log the current view of
        time from everything that may have logs generated.  This includes:
           Executors, where the test is running.
           Clients, where the IO is generated from.
        """
        funcs = list()
        args = list()
        funcs.append(self._log_executor_time)
        funcs.append(self._log_all_clients_time)
        Parallel(funcs=funcs, max_workers=len(funcs)).run_threads()

    def _log_executor_time(self):
        """
        Logs the executor time before starting the test in the local time
         of the executor using time.ctime()
        """
        hostname = None
        try:
            hostname = socket.gethostname()
            exec_time = time.ctime()
            print("{} is time on Executor {}".format(exec_time, hostname))
        except Exception as ex:
            # really broad exception used to prevent tests from being
            # executed for time logging.
            if hostname is None:
                self.logger.info(
                    "Failed to get local host name on executor.\n{}".format(ex))
            else:
                self.logger.info(
                    "Failed to get local time on executor {}\n".format(hostname,
                                                                       ex))

    def _log_all_clients_time(self):
        """
        Logs the client time in native time format, will need to be looked at
        for windows or ESXi support.
        """
        clients = qalib.client.list_from_equipment(self.equipment,
                                                   required=False)
        funcs = list()
        args = list()
        for client in clients:
            funcs.append(self._log_client_time)
            args.append([client])
        Parallel(funcs=funcs, args_list=args,
                 max_workers=len(funcs)).run_threads()

    def _log_client_time(self, client):
        """Will log the local time on a particular client."""
        try:
            c_time = client.run_cmd_check("date").strip()
            self.logger.info("{} time on Client {}".format(c_time, client.name))
        except Exception as ex:
            # really broad exception used to prevent tests from being
            # executed for time logging.
            self.logger.info(
                "Failed to get client time for {}.\n{}".format(client.name, ex))

    def log_instrumentation(self, key, value):
        """
        placeholder method to capture/store/print test case specific details
        Eventually all these information will be managed through DB.
        """
        self.logger.debug("{0}: {1} is {2}".format(self.id(), key, value))

    def setUp(self):
        """ Over-ride this to prepare the system for the test. """
        pass

    def tearDown(self):
        """ Over-ride this to clean up after the test. """
        pass
