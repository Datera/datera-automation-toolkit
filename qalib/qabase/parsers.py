# -*- coding: utf-8 -*-
"""
Location for generic parsing functions
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import logging

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())


# Utility parsers
def parse_table_colon_separated_no_headers(output):
    """
    Takes a table such as this one:

        Model:                 79
        Stepping:              0
        CPU MHz:               1200.035
        BogoMIPS:              4201.56
        Virtualization:        VT-x
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              256K
        L3 cache:              56320K
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              256K
        L3 cache:              56320K
        NUMA node0 CPU(s):     0-21,44-65
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              256K
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              256K
        NUMA node0 CPU(s):     0-21,44-65
        NUMA node1 CPU(s):     22-43,66-87

    And returns a dictionary like this:

        {u'architecture': u'x86_64',
         u'bogomips': u'4201.56',
         u'byte order': u'Little Endian',
         u'core(s) per socket': 22,
         u'cpu family': 6,
         u'cpu mhz': u'1200.445',
         u'cpu op-mode(s)': u'32-bit, 64-bit',
         u'cpu(s)': 88,
         u'l1d cache': u'32K',
         u'l1i cache': u'32K',
         u'l2 cache': u'256K',
         u'l3 cache': u'56320K',
         u'model': 79,
         u'numa node(s)': 2,
         u'numa node0 cpu(s)': u'0-21,44-65',
         u'numa node1 cpu(s)': u'22-43,66-87',
         u'on-line cpu(s) list': u'0-87',
         u'socket(s)': 2,
         u'stepping': 0,
         u'thread(s) per core': 2,
         u'vendor id': u'GenuineIntel',
         u'virtualization': u'VT-x'}
    """
    result = {}
    for line in (elem for elem in output.splitlines() if ":" in elem):
        key, value = line.split(":")
        try:
            result[key.strip().lower()] = int(value.strip())
        except ValueError:
            try:
                result[key.strip().lower()] = float(value.strip())
            except ValueError:
                result[key.strip().lower()] = value.strip()
    return result
