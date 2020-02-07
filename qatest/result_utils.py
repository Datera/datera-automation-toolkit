# -*- coding: utf-8 -*-
"""
Helper functions which may be useful when reporting test results
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import traceback


def _count_relevant_tb_levels(tb):
    """ How many stack trace levels do we care about? """
    length = 0
    while tb and not '__unittest' in tb.tb_frame.f_globals:
        length += 1
        tb = tb.tb_next
    return length

def exc_info_to_string(err, test):
    """ Converts sys.exc_info()-style tuple into a string """
    exctype, value, tb = err
    # Skip test runner traceback levels
    while tb and '__unittest' in tb.tb_frame.f_globals:
        tb = tb.tb_next
    if exctype is test.failureException:
        # Skip assert*() traceback levels
        length = _count_relevant_tb_levels(tb)
        msg_lines = traceback.format_exception(exctype, value, tb, length)
    else:
        msg_lines = traceback.format_exception(exctype, value, tb)
    return ''.join(msg_lines)

def get_test_case_name(test):
    """
    Returns the test run name, if it can be determined.
    e.g. if test.id() == "smoketests.test_smoke_001.TestCase.test_smoke_001",
    this would return "test_smoke_001".
    Parameter:
      test (unittest.TestCase)
    """
    if hasattr(test, 'id'):
        testcasename = test.id()
    elif hasattr(test, 'test') and hasattr(test.test, 'id'):
        testcasename = test.test.id()
    else:
        return "UNKNOWN"
    components = testcasename.split('.')
    if len(components) >= 3:
        testcasename = components[-1]
    return testcasename

def get_test_case_class(test):
    """
    Returns the test class name, if it can be determined.
    e.g. if test.id() == "smoketests.test_smoke_001.TestCase.test_smoke_001",
    this would return "TestCase".
    Parameter:
      test (unittest.TestCase)
    """
    if hasattr(test, 'id'):
        testcasename = test.id()
    elif hasattr(test, 'test') and hasattr(test.test, 'id'):
        testcasename = test.test.id()
    else:
        return "UNKNOWN"
    components = testcasename.split('.')
    if len(components) >= 3:
        testcasename = components[-2]
    return testcasename
