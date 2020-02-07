# -*- coding: utf-8 -*-
"""
Returns the product version dependant limits_constants.  This is based on the
running software version on the product and can change based on up when you ask
a cluster if upgrading.
"""
from qalib.qabase.limits import from_software_version

def from_api(api):
    """
    Returns a limits instance for determining limits based on currently running
    product version.
    Parameter:
      api (qalib.api.Api)
    """
    version = str(api.system.get()['sw_version'])
    return from_software_version(software_version=version)
