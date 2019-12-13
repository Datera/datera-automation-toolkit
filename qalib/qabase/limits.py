# -*- coding: utf-8 -*-
"""
Returns the product version dependant limits_constants.  This is based on the
running software version on the product and can change based on up when you ask
a cluster if upgrading.
"""
import logging

from qalib.qabase.limits_constants import override_3_0_0

__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

def _get_limits(software_version=None):
    """
    Private method to get the correct limits version based on product
        version
    """

    if software_version == "2.2.2":
        # return override_2_2_2.Limits_2_2_2()
        raise RuntimeError("This library expects at least version 3.0.0")
    elif software_version == "2.2.3":
        # return override_2_2_3.Limits_2_2_3()
        raise RuntimeError("This library expects at least version 3.0.0")
    else:
        return override_3_0_0.Limits_3_0_0()


def from_software_version(software_version):
    """
    Returns the product version dependant limits_constants.  This is based on
    the running software version on the product and can change based on up when
    you ask a cluster if upgrading.
    Args:
        software_version: (str) software version ex "3.1.2.0" or "2.2.7"
    """

    return _get_limits(software_version=software_version)
