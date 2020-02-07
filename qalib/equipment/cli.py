# -*- coding: utf-8 -*-
"""
Argument parsing for tools which need lab equipment
"""

from __future__ import (unicode_literals, print_function, division)

__copyright__ = "Copyright 2020, Datera, Inc."

import argparse
import re

from .equipment import EquipmentProvider
from .equipment import set_default_equipment_provider

_EQUIPMENT_ARG_DEST = 'equipment'

# Be afraid....be very afraid
CLI_RE = "(?:(?!@:)^([^:@]*)$|(?:(?!:)^([^:]*)@(.*)$|^([^:+]*):(.*)@(.*)$))"

"""
Regex to match one of these formats:
  hostname
  username@hostname
  username:password@hostname

(?:                 # Outer Conditional
  (?!@:)            # Negative Look-Ahead [:@] to check for if we should use another match
  ^([^:@]*)$        # Match any but [:@] (hostname)
  |                 # Else
  (?:               # Inner Conditional
    (?!:)           # Negative Look-Ahead
    ^([^:]*)@       # Match any but [:] (username)
    (.*)$           # Match any (hostname)
    |               # Else
    ^([^:+]*)       # Match any except more than one [:] (username)
    :(.*)@          # Match any (password)
    (.*)$           # Match any (hostname)
  )                 # End inner conditional
)                   # End outer conditional
"""


def _create_equipment_provider(namespace):
    """
    Creates an empty EquipmentProvider object in the argparse namespace which
    our argparse Action class below will add equipment to.  This is set as
    the default equipment provider.

    If called repeatedly, subsequent calls are a no-op.
    """
    equipment = getattr(namespace, _EQUIPMENT_ARG_DEST, None)
    if equipment is None:
        equipment = EquipmentProvider()
        set_default_equipment_provider(equipment)
        setattr(namespace, _EQUIPMENT_ARG_DEST, equipment)
    return equipment


class _ClientArgparseAction(argparse.Action):
    """ Parses --client """
    def __call__(self, parser, namespace, values, option_string=None):
        uname, pword, hname = None, None, None

        # Three possible match results:
        # hostname
        # username@hostname
        # username:password@hostname
        try:
            hname1, uname2, hname2, uname3, pword3, hname3 = re.match(
                CLI_RE, values).groups()
        except AttributeError:
            raise argparse.ArgumentError(
                "Client string did not match any supported format:\n"
                "hostname\nusername@hostname\nusername:password@hostname"
                "\n\n Client String: {}".format(values))

        if hname1:
            hname = hname1
        elif hname2:
            uname = uname2
            hname = hname2
        elif hname3:
            uname = uname3
            pword = pword3
            hname = hname3

        equipment_provider = _create_equipment_provider(namespace)
        equipment_provider._load_from_client_hostname(hname,
                                                      username=uname,
                                                      password=pword)


class _ClusterLabelArgparseAction(argparse.Action):
    """ Parses the -c/--cluster argument """
    def __call__(self, parser, namespace, values, option_string=None):
        equipment_provider = _create_equipment_provider(namespace)
        try:
            equipment_provider._load_from_str(values)
        except ValueError as ex:
            raise argparse.ArgumentError(self, str(ex))


class _ClusterIPArgparseAction(argparse.Action):
    """ Parses the --ip argument """
    """ Need to use --downcluster for now as its a single Node """
    def __call__(self, parser, namespace, values, option_string=None):
        equipment_provider = _create_equipment_provider(namespace)
        try:
            equipment_provider._load_from_cluster_Ip(values)
        except ValueError as ex:
            raise argparse.ArgumentError(self, str(ex))


class _EquipmentArgumentParser(argparse.ArgumentParser):
    """
    Custom argparse.ArgumentParser
    """
    def __init__(self, **kwargs):
        """ Initialize the equipment options for this parser """
        super(_EquipmentArgumentParser, self).__init__(**kwargs)
        self.set_defaults(equipment=None)
        group = self.add_argument_group(title="equipment")
        group.add_argument("-c", "--cluster", metavar='CLUSTER_LABEL',
                            help="Path to file containing " +
                           "cluster description " +
                           "(ending in .json)",
                           dest=argparse.SUPPRESS,
                           action=_ClusterLabelArgparseAction)

    def _verify_parsed_args(self, opts):
        """ Print an error and exit if opts.equipment unset """
        if getattr(opts, _EQUIPMENT_ARG_DEST, None) is None:
            self.error("No equipment specified")

    def parse_args(self, *args, **kwargs):
        """
        Parse args, print an error and exit if no equipment specified
        """
        opts = super(_EquipmentArgumentParser, self).parse_args(*args,
                                                                **kwargs)
        self._verify_parsed_args(opts)
        return opts

    def parse_known_args(self, *args, **kwargs):
        """
        Parse known args, print an error and exit if no equipment specified
        """
        opts, unparsed_args = super(_EquipmentArgumentParser,
                                    self).parse_known_args(*args, **kwargs)
        self._verify_parsed_args(opts)
        return (opts, unparsed_args)


def get_argument_parser(**kwargs):
    """
    Returns an argparse.ArgumentParser object for specifying lab equipment.

    This parser creates an EquipmentProvider object which is stored in the
    'equipment' parameter.

    This is the usual approach the test runner and most tools will use to
    interact with lab equipment.

    Parameters:
      See argparse.ArgumentParser for details
    """
    return _EquipmentArgumentParser(**kwargs)
