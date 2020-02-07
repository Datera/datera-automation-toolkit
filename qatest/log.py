# -*- coding: utf-8 -*-
"""
logging setup
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
__copyright__ = "Copyright 2020, Datera, Inc."

import logging
import logging.handlers
import os

import qalib.qabase.log
from qalib.qabase.log import LOGFILE_FORMAT as LOGFILE_FORMAT
from qalib.qabase.log import LOGFILE_SIZE as LOGFILE_SIZE
from qalib.qabase.log import CompressedFileHandler


def _remove_existing_handlers(logger):
    """ Removes all handlers from a logger object """
    handler_list = list(logger.handlers)
    for handler in handler_list:
        logger.removeHandler(handler)


def get_logdir():
    return qalib.qabase.log.make_logdir(toplevelname="qatest_results")


def configure_logging(logdir=None):
    """
    Configures the root logger
    Optional parameter:
      logdir (str) - Specify the results directory.  If None, one is
                     generated in a temporary directory ("/tmp")
    Returns the logdir
    """
    if not logdir:
        logdir = get_logdir()
    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    # Clean up and initialize root logger:
    rootlogger = logging.getLogger()
    _remove_existing_handlers(rootlogger)
    rootlogger.setLevel(logging.DEBUG)

    # Log all messages DEBUG and above to a file:
    debuglog = os.path.join(logdir, "qatest_debuglog.txt")
    if debuglog:
        #handler = logging.FileHandler(filename=debuglog, encoding="utf-8")
        handler = CompressedFileHandler(filename=debuglog, encoding="utf-8",
                                        max_bytes=LOGFILE_SIZE)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(LOGFILE_FORMAT)
        handler.setFormatter(formatter)
        rootlogger.addHandler(handler)
    return rootlogger
