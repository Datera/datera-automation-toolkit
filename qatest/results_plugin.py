# -*- coding: utf-8 -*-
"""
Provides the ResultsPlugin class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import unittest
import os

from .nose_bridge import nose
from . import result_utils


class ResultsPlugin(nose.plugins.Plugin):
    """
    A nose plugin to write a simple summary file of the test case results

    The file will look like:
      PASS test_smoke_001
      FAIL test_volume_002

    When a test starts, it's added to a dictionary.  As it runs, the
    dictionary is updated with the test result.  When the test is over,
    it is removed from the dictionary and its result is written to the
    results file.
    """
    enabled = True
    name = 'resultsplugin'
    score = 1540

    def __init__(self, logdir):
        super(ResultsPlugin, self).__init__()
        self.logdir = logdir
        self.results_txt = os.path.join(logdir, "qatest_results.txt")
        # Test result dictionary:
        self.test_result = dict()
        # Create the file:
        with open(self.results_txt, 'a'):
            pass

    def configure(self, options, conf):
        pass

    def options(self, parser, env):
        pass

    ########################################

    def _set_test_result(self, test, resultstr):
        """ Stores the test result in memory """
        if hasattr(test, 'id'):
            self.test_result[test.id()] = resultstr

    def _dump_test_result(self, test):
        """ Writes the test result to the file, then forgets it """
        if hasattr(test, 'id'):
            resultstr = self.test_result.pop(test.id())
            # If the test case looks like:
            #   "tests.smoketests.test_smoke_001.TestCase.test_run"
            # change it to "test_smoke_001" (just the script name)
            testcasename = result_utils.get_test_case_name(test)
            testcaseclass = result_utils.get_test_case_class(test)
            with open(self.results_txt, 'a') as f:
                f.write(
                    resultstr + " " + testcaseclass + "." + testcasename + "\n")

    def startTest(self, test):
        """ Called when the test case starts """
        self._set_test_result(test, 'UNKNOWN')

    def stopTest(self, test):
        """ Called when the test case ends """
        self._dump_test_result(test)

    def addSuccess(self, test):
        """ The test case succeeded """
        self._set_test_result(test, 'PASS')

    def addSkip(self, test, _reason):
        """ This method is obsolete; this is handled in addError() now """
        self._set_test_result(test, 'SKIP')

    def addError(self, test, err):
        """ The test case hit an exception """
        # Is it a skip?:
        exctype, _value, _tb = err
        if issubclass(exctype, unittest.SkipTest):
            self._set_test_result(test, 'SKIP')
            return
        # Regular error:
        self._set_test_result(test, 'ERROR')

    def addFailure(self, test, _err):
        """ The test case failed """
        self._set_test_result(test, 'FAIL')

    def addExpectedFailure(self, test, _err):
        """ The test case failed, but the failure was expected """
        self._set_test_result(test, 'FAIL')

    def addUnexpectedSuccess(self, test):
        """ The test case succeeded, but was expected to fail """
        self._set_test_result(test, 'PASS')
