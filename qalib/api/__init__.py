"""
API abstraction layer.

See the api2 sub-package documentation for latest usage.
"""
import urllib3
import logging
import threading

import dfs_sdk
from dfs_sdk.hooks.cleanup import CleanupHandler as SDKCleanupHandler

__copyright__ = "Copyright 2020, Datera, Inc."

###############################################################################
# this is being done as the API will generate insecure warnings when using
# the sdk directly.
urllib3.disable_warnings()
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

# preventing concurrent access to getting an api object.
API_LOCK = threading.Lock()

def sdk_from_cluster(cluster,
                     secure=True,
                     tenant="/root",
                     ldap_server=None,
                     version="v2.2",
                     refresh=True):
    """
    Gets an external Python SDK object of the requested version with
    appropriate CleanupHandler registered from the provided cluster object
    Parameter:
      equipment (qalib.equipment.ClusterEquipment)
    Optional parameters:
      secure (boolean) - Use HTTPS instead of HTTP. Defaults to HTTPS
      ldap_server (string) - LDAP server name
      tenant (string) - The tenant to use for this SDK object (default /root)
      version (string) - The API version to use for requests (default v2.2)
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    ch = SDKCleanupHandler()
    fe = FromEntityHook(cluster)
    with API_LOCK:
        sdk = dfs_sdk.get_api(cluster.mgmt_node,
                              cluster.admin_user,
                              cluster.admin_password,
                              version,
                              tenant=tenant,
                              ldap_server=ldap_server,
                              hooks=[ch, fe],
                              refresh=refresh,
                              disable_log=True,
                              secure=secure)
    fe.add_sdk(sdk)
    # Patch the cleanup function into the SDK itself so it mimics the old api
    # cleanup functionality
    sdk.cleanup = ch.cleanup
    if hasattr(sdk, '_context'):
        setattr(sdk._context, 'cluster', cluster)
    return sdk


def get_clusterequipment(api):
    """
    Tests and tools should not use this directly.  This is for hooking the
    libraries together.

    Parameter:
      api (qalib.api.Api)

    Returns a qalib.corelibs.equipmentprovider.ClusterEquipment object
    """
    if hasattr(api, '_context'):
        return api._context.cluster
    try:
        return api._clusterequipment
    except AttributeError:
        raise ValueError("Not an Api object")


def from_entity(resource, developed_for=None, **kwargs):
    """
    Tests and tools do not normally use this directly (although they can).
    This is for mainly hooking the libraries together.

    For a given Entity, returns an API object connected to the same cluster
    """
    if hasattr(resource, '_context'):
        if hasattr(resource._context, 'version'):
            if not developed_for:
                developed_for = getattr(resource._context, 'version', 'v2')
            if developed_for != 'v2.2':
                api = resource._context.api
        else:
            raise AttributeError("Failed to identify the API version from "
                                 "Response:{}"
                                 .format(resource._context.__dict__))
    else:
        try:
            api = resource._api
            if not developed_for:
                developed_for = 'v1'
        except AttributeError:
            raise ValueError("Not an ApiResource object: %s" % repr(resource))

    if developed_for == 'v2.2':
        # TODO[jsp]: probably call get_clusterequipment here too
        cluster = resource._context.cluster
    else:
        cluster = get_clusterequipment(api)
    api_obj = sdk_from_cluster(cluster, version=developed_for, **kwargs)
    return api_obj


class FromEntityHook(dfs_sdk.hooks.base.BaseHook):
    """
    SDK hook for compatibility with from_entity and related methods
    """
    def __init__(self, cluster):
        self.cluster = cluster
        self.sdk = None

    def add_sdk(self, sdk):
        self.sdk = sdk

    def supported_versions(self):
        """ Called to check if hook supports current API version.
        Returns: list/tuple of supported API version strings
        Eg: ("v2.1", "v2.2")
        """
        return "v2.1", "v2.2"

    def prepare_entity(self, entity):
        """ Called after an entity has been retrieved from system
        Returns: Modified entity (if any modification)
        """
        entity._api = self.sdk
        entity._context = entity.context
        entity._context.cluster = self.cluster
