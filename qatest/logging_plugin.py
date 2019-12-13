# -*- coding: utf-8 -*-
"""
Provides the LoggingPlugin class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import unittest
import logging
import socket
import getpass

from .nose_bridge import nose
from . import result_utils

__copyright__ = "Copyright 2020, Datera, Inc."


class LoggingPlugin(nose.plugins.Plugin):
    """
    A nose plugin to log test results

    The framework and other plugins are responsible for where these log
    messages go.
    """
    enabled = True
    name = 'loggingplugin'
    score = 1810

    def __init__(self):
        super(LoggingPlugin, self).__init__()
        self.logger = logging.getLogger(__name__)

    def configure(self, options, conf):
        """ Enable this plugin """
        pass

    def options(self, parser, env):
        """ Enable this plugin """
        pass

    ########################################

    def _log_test_stacktrace(self, test, err):
        """ Dump an error message with a stack trace """
        errstr = result_utils.exc_info_to_string(err, test)
        self.logger.warning(errstr.rstrip())

    def _log_test_skip(self, _test, reason):
        """ Log a skip message """
        self.logger.debug("SkipTest: {0!r}".format(reason))
        self.logger.debug("SKIP")

    ########################################

    def begin(self):
        """ Called at the beginning of the test run """
        pass

    def finalize(self, result):
        """ Called at the end of the test run """
        pass

    def beforeTest(self, test):
        """ Called before the test case is run """
        pass

    def afterTest(self, test):
        """ Called after the test case is run """
        pass

    def startTest(self, test):
        """ Called when the test case starts """
        testcasename = result_utils.get_test_case_name(test)
        msg = "BEGIN test: " + testcasename + " (" + test.id() + ")"
        self.logger.debug(msg)
        msg = "Executor hostname: " + socket.gethostname()
        self.logger.debug(msg)
        msg = "Running as user: " + getpass.getuser()
        self.logger.debug(msg)

    def stopTest(self, test):
        """ Called when the test case ends """
        testcasename = result_utils.get_test_case_name(test)
        msg = "END test: " + testcasename + " (" + test.id() + ")"
        self.logger.debug(msg)

    def addSuccess(self, _test):
        """ The test case succeeded """
        self.logger.debug("PASS")

    def addSkip(self, test, reason):
        """ This method is obsolete; this is handled in addError() now """
        self._log_test_skip(test, reason)

    def addError(self, test, err):
        """ The test case hit an exception """
        exctype, _value, _tb = err
        if issubclass(exctype, unittest.SkipTest):
            # Test skip (not really an error):
            _exctype, value, _tb = err
            reason = value.message
            self._log_test_skip(test, reason)
        else:
            # Regular error:
            self._log_test_stacktrace(test, err)
            self.logger.debug("ERROR")

    def addFailure(self, test, err):
        """ The test case failed """
        self._log_test_stacktrace(test, err)
        self.logger.debug("FAIL")

    def addExpectedFailure(self, test, err):
        """ The test case failed, but the failure was expected """
        self._log_test_stacktrace(test, err)
        self.logger.debug("Expected failure")
        self.logger.debug("FAIL")

    def addUnexpectedSuccess(self, _test):
        """ The test case succeeded, but was expected to fail """
        self.logger.debug("Unexpected success")
        self.logger.debug("PASS")
