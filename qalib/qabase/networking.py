# -*- coding: utf-8 -*-
"""
Common network functions
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import socket
import time
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
        logger.addHandler(logging.NullHandler())


def gethostbyname_retry(hostname):
    """ Calls socket.gethostbyname() with retries for transient errors """
    attempts = 0
    attempts_max = 30
    while True:
        attempts += 1
        try:
            return socket.gethostbyname(hostname)
        except (socket.error, socket.gaierror) as ex:
            if ex.errno == socket.EAI_AGAIN and attempts <= attempts_max:
                # Transient DNS error, retry
                time.sleep(2)
                continue
            else:
                logger.error("Error looking up hostname " + repr(hostname) +
                             ": " + str(ex))
                raise
