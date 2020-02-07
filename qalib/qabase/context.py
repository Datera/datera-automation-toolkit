# -*- coding: utf-8 -*-
"""
Common context manager libraries
"""

import time
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class TimeIt(object):
    """ Measure the elapse time inside the context
        Eg.
           with TimeIt() as t
               run_like_you_mean_it()
           logger.info("elapse time : {}".format(t.interval))
    """

    def __init__(self):
        self.interval = 0

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        logger.debug("[TimeIt] elapse time : [{}] seconds"
                     .format(self.interval))
