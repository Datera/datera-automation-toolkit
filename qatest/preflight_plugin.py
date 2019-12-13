# -*- coding: utf-8 -*-
"""
Provides the TestcaseLogfilePlugin class
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import qalib.preflight
from .nose_bridge import nose


class PreflightPlugin(nose.plugins.Plugin):
    """
    Nose plugin to check that cluster is ready to be tested before each test case
    """
    enabled = True
    name = 'preflightplugin'
    score = 1115 # must be greater than testcase_log_plugin

    def __init__(self, equipment):
        super(PreflightPlugin, self).__init__()
        self.preflight_helper = qalib.preflight.ClusterReady(equipment)

    def configure(self, _options, _conf):
        pass

    def options(self, _parser, _env):
        pass

    def beforeTest(self, _test):
        self.preflight_helper.rotate_logs()
        self.preflight_helper.ensure_cluster_ready()
