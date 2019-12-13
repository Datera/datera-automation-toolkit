# -*- coding: utf-8 -*-
"""
Provides the OutputPlugin class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import unittest
import logging
import sys
import os
import time
import socket

from qalib.qabase.log import QATEST_LOGSCREEN_FORMAT as _CONSOLE_FORMAT
from .nose_bridge import nose
from . import result_utils
from .null_output import NullOutput

__copyright__ = "Copyright 2020, Datera, Inc."


class OutputPlugin(nose.plugins.Plugin):
    """
    A nose plugin to produce prettier output than nose's default TestResult.

    The test harness, test cases, and other plugins are responsible for
    generating the actual log messages (e.g. stack traces when tests fail).
    This plugin just determines how to display them on the screen.
    """
    enabled = True
    name = 'outputplugin'
    score = 1115

    def __init__(self, loglevel=logging.INFO, logdir=None):
        super(OutputPlugin, self).__init__()
        self.stream = sys.stderr
        self.logdir = logdir
        self.loglevel = loglevel
        self.num_executed = 0
        self.num_errors = 0
        self.num_failures = 0
        self.num_expected_failures = 0
        self.num_unexpected_successes = 0
        self.num_skipped = 0
        try:
            self.executor_name = socket.gethostname()
        except socket.error:
            self.executor_name = ""

    def configure(self, options, conf):
        pass

    def options(self, parser, env):
        pass

    def setOutputStream(self, stream):
        """
        Steal stdout for ourselves; all other plugins' output will be discarded
        """
        self.stream = stream
        return NullOutput()

    ########################################

    def _was_successful(self):
        """ Was this test run a success?  """
        if self.num_errors > 0 or self.num_failures > 0:
            return False
        else:
            return True

    def _print_separator(self, point, start=''):
        """ Print a horizontal line """
        line = start
        line += point * 70
        line = line[:70]
        self.stream.write(line + "\n")

    ########################################

    def _set_onscreen_logging(self):
        """
        Creates a root logger handler to print log messages to stdout based
        on loglevel.  Removes any previous handlers.

        (We call this multiple times, because each time we call this,
        sys.stdout might have a different value due to other plugins.)
        """
        stream = sys.stdout
        rootlogger = logging.getLogger()
        remove_handlers = [handler for handler in rootlogger.handlers
                           if isinstance(handler, OutputStreamHandler)]
        for handler in remove_handlers:
            rootlogger.removeHandler(handler)
        handler = OutputStreamHandler(stream=stream)
        handler.setLevel(self.loglevel)
        formatter = logging.Formatter(_CONSOLE_FORMAT)
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        rootlogger.addHandler(handler)

    ########################################

    def begin(self):
        """ Print a banner at the start of the test run """
        self._print_separator('=')
        self.stream.write("Datera Test Run\n")
        # TODO: describe equipment, etc
        self.stream.write("Executor: " + self.executor_name + "\n")
        if self.logdir:
            self.stream.write("Logs: " + self.logdir + os.sep + "\n")
        self._print_separator('=')
        self._set_onscreen_logging()

    def beforeTest(self, test):
        """ Print a header before each test case starts """
        self._set_onscreen_logging()
        testcasename = result_utils.get_test_case_name(test)
        testcaseid = test.id()
        if hasattr(test, 'test'):
            if isinstance(test.test, nose.failure.Failure):
                testcaseaddress = test.test.address()
                if testcaseaddress:
                    (_filename, module, _funcname) = testcaseaddress
                    if module:
                        testcasename = module.split(".")[-1]
                        testcaseid = "Failure"

        self._print_separator('-', start='+')
        self.stream.write("| " + testcasename + " (" + testcaseid + ")\n")
        doc_first_line = test.shortDescription()
        if doc_first_line:
            self.stream.write("| " + doc_first_line + '\n')
        self._print_separator('-', start='+')

    def afterTest(self, _test):
        """ When a test case finishes, we don't need to print anything """
        self._set_onscreen_logging()

    def startTest(self, _test):
        """ Keep count """
        self.num_executed += 1

    def stopTest(self, test):
        """ Called when the test case has completed """
        pass

    def addSuccess(self, _test):
        """ The test case succeeded """
        self.stream.write("PASS\n")

    def addSkip(self, _test, reason):
        """ This method is obsolete; this is handled in addError() now """
        self.num_skipped += 1
        self.stream.write("SkipTest: {0!r}\n".format(reason))
        self.stream.write("SKIP\n")

    def addError(self, _test, err):
        """ The test case hit an exception """
        # Is it a skip?:
        exctype, value, _tb = err
        if issubclass(exctype, unittest.SkipTest):
            self.num_skipped += 1
            self.stream.write("SkipTest: {0!r}\n".format(value.message))
            self.stream.write("SKIP\n")
            return
        # Regular error:
        self.num_errors += 1
        self.stream.write("ERROR\n")

    def addFailure(self, _test, _err):
        """ The test case failed """
        self.num_failures += 1
        self.stream.write("FAIL\n")

    def addExpectedFailure(self, _test, _err):
        """ The test case failed, but the failure was expected """
        self.num_expected_failures += 1
        self.stream.write("Expected failure\n")

    def addUnexpectedSuccess(self, _test):
        """ The test case succeeded, but was expected to fail """
        self.num_unexpected_successes += 1
        self.stream.write("Unexpected success\n")

    def finalize(self, _result):
        """ Prints a final summary of the test run """
        self._print_separator('_')
        self.stream.write("Executor: " + self.executor_name + "\n")
        if self.logdir:
            self.stream.write("Logs: " + self.logdir + os.sep + "\n")
        msg = "Ran %d test%s" % (self.num_executed,
                                 self.num_executed != 1 and "s" or "") + "\n"
        if self._was_successful():
            msg += "OK"
        else:
            msg += "FAILED"
        infos = []
        failed = self.num_failures
        errored = self.num_errors
        expectedFails = self.num_expected_failures
        unexpectedSuccesses = self.num_unexpected_successes
        skipped = self.num_skipped
        if failed > 0:
            infos.append("failures=%d" % failed)
        if errored > 0:
            infos.append("errors=%d" % errored)
        if skipped:
            infos.append("skipped=%d" % skipped)
        if expectedFails:
            infos.append("expected failures=%d" % expectedFails)
        if unexpectedSuccesses:
            infos.append("unexpected successes=%d" % unexpectedSuccesses)
        if infos:
            msg += " (%s)" % (", ".join(infos),)
        self.stream.write(msg + '\n')


class OutputStreamHandler(logging.StreamHandler):
    """
    Sub-class logging.StreamHandler so we can recognize handlers added
    by this plugin
    """
    pass
