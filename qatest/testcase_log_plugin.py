# -*- coding: utf-8 -*-
"""
Provides the TestcaseLogfilePlugin class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import logging
import os
import time

from qalib.qabase.log import LOGFILE_FORMAT
from qalib.qabase.log import LOGFILE_SIZE
from qalib.qabase.log import CompressedFileHandler
from .nose_bridge import nose
from . import result_utils


class TestcaseLogfilePlugin(nose.plugins.Plugin):

    """
    A nose plugin to create an individual logfile for each test case
    """
    enabled = True
    name = 'testcaselogfileplugin'
    score = 1110

    def __init__(self, baselogdir):
        super(TestcaseLogfilePlugin, self).__init__()
        self.baselogdir = baselogdir # /tmp/qatest_results/20200130.1402.41.0
        self.test_handlers = dict()

    def configure(self, options, conf):
        pass

    def options(self, parser, env):
        pass

    ########################################

    def beforeTest(self, test):
        """ Add a testcase-specific logging handler """
        if not self.baselogdir:
            return

        testcase = test.test
        rootlogger = logging.getLogger()

        testcasename = ".".join((result_utils.get_test_case_class(test),
                                 result_utils.get_test_case_name(test)))

        # Create the <testname>/ sub-folder
        # log_upload_plugin requires this to be set
        testcase.logdir = os.path.join(self.baselogdir, get_test_dest_dir(testcasename))
        if not os.path.exists(testcase.logdir):
            try:
                os.mkdir(testcase.logdir)
            except OSError:
                if not os.path.exists(testcase.logdir):
                    raise

        # Create the <testname>/testlogs/ sub-folder for test case logfiles
        testlogdir = os.path.join(testcase.logdir, "testlogs")
        if not os.path.exists(testlogdir):
            try:
                os.mkdir(testlogdir)
            except OSError:
                if not os.path.exists(testlogdir):
                    raise

        # Needed by FIO test cases that need to save additional logs to logdir
        testcase.testlogdir = testlogdir

        # Create a unique logfile name, even if this test case is run
        # more than once in a single test run (weird, but possible)
        index = 0
        while True:
            debuglog = testcasename + ".DEBUG." + str(index) + ".txt"
            infolog = testcasename + ".INFO." + str(index) + ".txt"
            debuglogpath = os.path.join(testlogdir, debuglog)
            infologpath = os.path.join(testlogdir, infolog)
            if os.path.exists(debuglogpath) or os.path.exists(infologpath):
                index = index + 1
                continue
            else:
                break
        handlers = list()
        self.test_handlers[test] = handlers

        # DEBUG
        #handler = logging.FileHandler(filename=debuglogpath, encoding="utf-8")
        handler = CompressedFileHandler(filename=debuglogpath,
                                        encoding="utf-8",
                                        max_bytes=LOGFILE_SIZE)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(LOGFILE_FORMAT))
        rootlogger.addHandler(handler)
        handlers.append(handler)
        # INFO
        handler = logging.FileHandler(filename=infologpath, encoding="utf-8")
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(LOGFILE_FORMAT))
        rootlogger.addHandler(handler)
        handlers.append(handler)

    def afterTest(self, test):
        """ Remove the logging handlers """
        rootlogger = logging.getLogger()
        for handler in self.test_handlers.pop(test, []):
            if handler:
                rootlogger.removeHandler(handler)

def get_test_dest_dir(testcase_id):
    """
    Return a uri to upload to based on destination uri
    plus the id of the current test case.
    Uses Jenkins environment variables if they are set
    """
    return testcase_id.split('.')[-1] + "." + str(int(time.time()))
