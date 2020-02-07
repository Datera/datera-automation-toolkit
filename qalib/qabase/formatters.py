# -*- coding: utf-8 -*-
"""
Code for formatting/transforming data
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

from .constants import KB, MB, GB, TB, PB

__copyright__ = "Copyright 2020, Datera, Inc."


def human_readable_size(size):
    """
    Convert size in bytes to a human-readable str.
    The exact format may change, so this string should not be parsed.
      size (int) - A size in bytes
    Examples:
      >>> human_readable_size(512)
      '512'
      >>> human_readable_size(1024)
      '1KB'
      >>> human_readable_size(1200)
      '1.17KB'
      >>> human_readable_size(9999999999999)
      '9.09TB'
    """
    size = int(size)
    for unitsize, unitstr in ((PB, "PB"),
                              (TB, "TB"),
                              (GB, "GB"),
                              (MB, "MB"),
                              (KB, "KB")):
        if size >= unitsize:
            size_converted = float(size) / unitsize
            size_str = "{:.2f}".format(size_converted)
            size_str = size_str.rstrip('0').rstrip('.')
            return str(size_str + unitstr)
    return str(size)


def human_readable_time_from_seconds(seconds, depth=4):
    """
    Convert seconds  to a human-readable str.
    The exact format may change, so this string should not be parsed.
      seconds (int) - a time in seconds
      depth (int) max larged units to report.
    Examples:
        In [2]: hrts(4)
        Out[2]: u'4 seconds'

        In [3]: hrts(400)
        Out[3]: u'6 minutes 40 seconds'

        In [4]: hrts(4000)
        Out[4]: u'1 hours 6 minutes 40 seconds'

    if depth = 2:
        In [3]: hrts(400)
        Out[3]: u'6 minutes 40 seconds'

        In [4]: hrts(4000)
        Out[4]: u'1 hours 6 minutes'
      """
    seconds = int(seconds)
    if seconds == 0:
        return "0 seconds"
    # hours
    h = 0
    # minutes
    m = 0
    # seconds
    s = 0
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    used_depth = 0
    hrt = ""
    if d > 0:
        if d > 1:
            hrt += "{} days".format(d)
        else:
            hrt += "{} day".format(d)
        used_depth += 1
        if used_depth >= depth:
            return hrt
        else:
            hrt += ", "
    if h > 0:
        if h > 1:
            hrt += "{} hours".format(h)
        else:
            hrt += "{} hour".format(h)
        used_depth += 1
        if used_depth >= depth:
            return hrt
        else:
            hrt += ", "
    if m > 0:
        if m > 1:
            hrt += "{} minutes".format(m)
        else:
            hrt += "{} minute".format(m)
        used_depth += 1
        if used_depth >= depth:
            return hrt
        else:
            hrt += ", "
    if s > 0:
        if s > 1:
            hrt += "{} seconds".format(s)
        else:
            hrt += "{} second".format(s)
    return hrt
