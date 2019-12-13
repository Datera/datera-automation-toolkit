# -*- coding: utf-8 -*-
"""
Utility parser for different operating systems.  This object is included
with anything that inherits from System (eg, Client and Whitebox)

Example Module Usage:

    client = qalib.client.from_equipment(e)
    cpu_info = client.util.get_cpuinfo()
    print(cpu_info['flags'])

Utility parsers that function for Linux and BSD can go in Posix so they're
available for either class to use.

"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import logging

from qalib.qabase.parsers import parse_table_colon_separated_no_headers

LOG = logging.getLogger(__name__)


def get_util_parser(equipment):
    # Logic to figure out what system we're running on
    result = None
    try:
        uname = equipment.run_cmd_check("uname -a").lower()
    except EnvironmentError:
        LOG.info("This OS is not current supported in the util parsers module")
        return None
    if "linux" in uname:
        result = Linux(equipment)
    return result


class Posix(object):

    def __init__(self, equipment):
        self.equipment = equipment


class Linux(Posix):

    def __init__(self, equipment):
        super(self.__class__, self).__init__(equipment)

    def get_lscpu(self):
        """
        Parses the output of lscpu (with no arguments)
        :param output: The output of `lscpu`
        :returns: A dictionary with this structure

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
        # TODO[jsp]: this won't fly for storage nodes
        output = self.equipment.run_cmd_check("lscpu")
        return parse_table_colon_separated_no_headers(output)
