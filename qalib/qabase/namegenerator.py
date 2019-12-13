"""
This module has methods that can generate unique name
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import time
import random
import uuid
import threading

_module_lock = threading.Lock()


_last_unique_timestamp = None  # used in name_generator() to ensure uniqueness

def name_generator(input_string="name-generator"):
    """
    This method makes a name unique by appending the current timestamp.
    If no string is provided, "name-generator" is used as prefix
    """
    global _last_unique_timestamp  # guarantee uniqueness
    with _module_lock:
        while True:
            unique_timestamp = repr(time.time())
            if unique_timestamp != _last_unique_timestamp:
                _last_unique_timestamp = unique_timestamp
                return input_string + "-" + unique_timestamp
            else:
                continue  # try again


def uuid_generator():
    """Generate a random UUID. Return a string.

    UUID Format:
        XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX where X is lower-case hex digit.
    """
    return str(uuid.uuid1())


# TODO[jsp]: this may be more complicated for our needs than necessary?
def initiator_name_generator():
    """
    Returns a str which can be used as an iSCSI initiator name

    Randomly chooses between IQN format and Microsoft's version of EUI-64.
    """
    if random.randint(0, 1):
        return iqn_generator()
    else:
        return eui64_initiator_name_generator(microsoft_format=True)


def iqn_generator():
    """
    Consider calling initiator_name_generator() instead of this.

    Generate a random iqn. iqn format must match
            iqn-YYYY-MM.<domain>.<domain>:X
            where the whole length of iqn is 16-233.

    Returns: iqn as string.
    """
    IQN = "iqn."
    DOMAIN = ".com.datera:"
    today = unicode(time.gmtime().tm_year) + '-' + \
        "{:0>2d}".format(time.gmtime().tm_mon)
    return str(IQN + today + DOMAIN + uuid_generator())


def eui64_initiator_name_generator(microsoft_format=True):
    """
    Consider calling initiator_name_generator() instead of this.

    Generates an EUI-64 format initiator name.
    Returns a str of the form 'eui.B35EAA83EFA798ED'

    Parameter:
      microsoft_format (bool) - If True, return Microsoft-compatible names
                                with all-lowercase hex digits.
                                If False, return RFC-compliant names.

    Note: these EUIs are reserved, and are invalid initiator names:
      eui.0000000000000000
      eui.FFFFFFFFFFFFFFFF
    Note: Encapsulated EUI-48 would have this form:
      eui.******FFFF******
    Note: Encapsulated MAC-48 would have this form:
      eui.******FFFE******
    References:
      https://standards.ieee.org/develop/regauth/tut/eui64.pdf
      https://tools.ietf.org/html/rfc3721
    """
    # return "eui." + "%016X" % random.randint(1, (2**64 - 2))
    eui0_eui1_eui2 = "%06X" % random.randint(1, 2 ** 24 - 2)  # 000001 - FFFFFE
    eui3_eui4 = "%04X" % random.randint(0, 2 ** 16 - 2)  # 0000 - FFFE
    eui5_eui6_eui7 = "%06X" % random.randint(0, 2 ** 24 - 1)  # 000000 - FFFFFF
    hexportion = eui0_eui1_eui2 + eui3_eui4 + eui5_eui6_eui7
    if microsoft_format:
        hexportion = hexportion.lower()
    return "eui." + hexportion
