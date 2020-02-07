#!/usr/bin/env python
# -*- coding: utf-8 -*-
import qatest

__copyright__ = "Copyright 2020, Datera, Inc."

class TestCase(qatest.TestCase):
    """
    Test to make sure we can collect logs from test environment
    """
    def test_logcollect(self):
        self.logger.info("Testing logcollect")
        self.fail()
