# -*- coding: utf-8 -*-
"""
Provides access to the EquipmentProvider object, which is used as a
starting point for instantiating most other QA library objects.

Test cases should not use this package directly.
This can be used by tools, and for debugging.

Example:
  >>> import qalib.equipment
  >>> equipment = qalib.equipment.from_str("tlx20s")
  ...
  >>> cluster = equipment.get_cluster(required=True)
  >>> import qalib.whitebox
  >>> for whitebox in qalib.whitebox.list_from_cluster(cluster)
  ...     whitebox = qalib.whitebox.from_server(server)
  ...     print whitebox.run_cmd_check("hostname")
  ...
  >>> import qalib.client
  >>> client = qalib.client.from_equipment(equipment)
  >>> print client.run_cmd_check("hostname")

This package should be thought of as an adapter between external
descriptions of test equipment (command-line arguments, databases,
config files, etc) and internal representations (ClusterEquipment
and ClientEquipment objects).

This provides an argument parser which creates an EquipmentProvider object.
Tests and tools should treat this data as opaque.
It can be passed to libraries to generate objects for interacting with
lab equipment.

This package depends on the qalib.corelibs.credentials package.
This package depends on the qalib.qabase.siteconfig package.
This package depends on the qalib.corelibs.systemconnection package.
"""
__copyright__ = "Copyright 2020, Datera, Inc."


from .equipment import EquipmentProvider
from .clientequipment import ClientEquipment
from .clusterequipment import ClusterEquipment

from qalib.qabase.exceptions import EquipmentNotFoundError

from .equipment import get_default_equipment_provider
from .equipment import set_default_equipment_provider

from .cli import get_argument_parser
from .equipment import from_str

__all__ = ['get_argument_parser',
           'EquipmentProvider',
           'EquipmentNotFoundError',
           'ClientEquipment',
           'ClusterEquipment',
           'get_default_equipment_provider',
           'set_default_equipment_provider',
           'from_str',
]
