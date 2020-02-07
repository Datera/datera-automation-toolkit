#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import argparse
import os
import sys
import requests

from qatest.logging_plugin import LoggingPlugin
from qatest.preflight_plugin import PreflightPlugin
from qatest.testcase_log_plugin import TestcaseLogfilePlugin
from qatest.output_plugin import OutputPlugin
from qatest.results_plugin import ResultsPlugin
from qatest.log_upload_plugin import LogUploadPlugin
import qalib
import qalib.client
import qalib.equipment
import qatest.log as log

from qatest.nose_bridge import nose


def get_args():
    """
    Parses and return commandline arguments
    """
    equipment_parser = \
        qalib.equipment.get_argument_parser(add_help=False)
    parser = argparse.ArgumentParser(parents=[equipment_parser])
    parser.add_argument("--test-logcollect",
                        help="Run a test to verify logs can be collected from the test system. No other tests will run",
                        action='store_true',
                        default=False)

    args = parser.parse_args()
    return args

def main():
    opts = get_args()

    logdir=log.get_logdir()
    log.configure_logging(logdir=logdir)

    addplugins = [ LoggingPlugin(),
                    TestcaseLogfilePlugin(logdir),
                    OutputPlugin(logdir=logdir, loglevel=logging.INFO),
                    ResultsPlugin(logdir),
                    LogUploadPlugin(logdir, opts.equipment)
    ]

    progname = os.path.basename(sys.argv[0])
    if opts.test_logcollect:
        testlist = ["tests/test_logcollect.py"]
    else:
        testlist = ["tests/test_io_basic.py"]
        addplugins.append(PreflightPlugin(opts.equipment))

    nose_argv = [progname,
                 "--no-byte-compile",
                 "--nocapture",
                 "--nologcapture",
                 "--exe",
                 "--verbosity", "2"]
    #             "--with-xunit", "--xunit-file", xunit_result,
    nose_argv = nose_argv + testlist

    #
    #  Here we go:
    #
    testprog = nose.main(argv=nose_argv,
                         addplugins=addplugins,
                         exit=False)
    return testprog.success is True

if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings(
        requests.packages.urllib3.exceptions.InsecureRequestWarning)
    logging.getLogger("requests").setLevel(logging.WARNING)

    main()
