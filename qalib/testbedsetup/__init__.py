"""
This is the orchestartion library responsible for maintaining state during
test runs / setups etc.
"""

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

from pprint import pformat
from random import randint
import itertools
import logging
import random
import threading
import time

from .base import BaseTestBedSetup
from qalib import api
from qalib import client
from qalib.qabase import testrun_params
from qalib.qabase.constants import GB
from qalib.qabase.context import TimeIt
from qalib.qabase.exceptions import ApiConflictError, ApiNotFoundError
from qalib.qabase.formatters import human_readable_time_from_seconds as hrts
from qalib.qabase.namegenerator import name_generator
from qalib.qabase.polling import poll
import qalib.qabase.features
from qalib.qabase.threading import Parallel
from qalib.qabase.algorithms import round_robin


logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

STORAGE_INSTANCE_AVAILABLE_TIMEOUT = 180
VOLUME_AVAILABLE_TIMEOUT = 300


class TestBedSetup(BaseTestBedSetup):
    # pylint: disable=too-many-instance-attributes
    def __init__(self, equipment, app_templates, clients=None,
                 required_clients=True, iscsi_redirect=None,
                 client_optimized=None, iscsi_rescan=None,
                 failure_domains=None, fd_teardown=None,
                 tenant_path=None, fill_volumes=None, dirty_spans=None):
        """
        Its not recommended to use this method directly. Please use the class
        methods to use this class.
        """
        self._equipment = equipment
        # testbed setup is the entry/exit point in a test, making a copy
        # of cluster object to keep it unique for a setup
        self._cluster = equipment.get_cluster().copy()
        sdk = qalib.api.sdk_from_cluster(self._cluster)
        self._api = sdk
        self.features = qalib.qabase.features.from_api(sdk)
        if clients:
            self._clients = clients
        else:
            self._clients = client.list_from_equipment(
                self._equipment,
                required=required_clients)
        if tenant_path is None:
            self.tenant_path = "/root"
        else:
            self.tenant_path = tenant_path
        self._fill_volumes = fill_volumes
        self._dirty_spans = dirty_spans
        self._app_templates_container = app_templates
        if failure_domains is None:
            failure_domains = []
        self._failure_domain_containers = failure_domains
        # If True, will reset failure_domains to previous values when cleanup()
        # is called.
        if fd_teardown is None:
            fd_teardown = False
        self._fd_teardown = fd_teardown
        self._prev_failure_domains = {}
        if iscsi_redirect is None:
            iscsi_redirect = testrun_params.DEFAULT_ISCSI_REDIRECT
        self._iscsi_redirect = iscsi_redirect
        self._single_node = False
        self._block_io = True
        self._targets = []
        self._initiator_group = None
        self._client_ifaces = {}
        # create volumes
        self.volumes = []
        # create exports
        self.storage_instances = []
        # app instances
        self.app_instances = []
        # failure domains keyed to node uuids
        self.failure_domains = {}
        self._thread_scratchpad = []
        self._lock = threading.Lock()
        self._client_si_map = {}
        self._name = name_generator('tbs')
        self._client_optimized = client_optimized
        self._iscsi_rescan = iscsi_rescan

    def is_on_hardware(self):
        return self._cluster.is_on_hardware()

    def configure(self):
        """
        configures cluster and client
        """
        with TimeIt() as t:
            self.configure_cluster()

        logger.info("{} Finish configuring cluster with {} volumes,"
                    " took {}"
                    .format(self._name, len(self.volumes), hrts(t.interval)))

        if self._client_optimized is True:
            with TimeIt() as t:
                self.configure_clients_from_client_si_map()

            logger.info("[{}] Finish attaching targets, took [{:.1f}] seconds"
                        .format(self._name, t.interval))
        else:
            self.configure_clients()

        if self._fill_volumes:
            self._fill_volumes()
        if self._dirty_spans:
            self._dirty_spans()

    def _populate_targets_from_si_list(self, client, si_list=None):
        _targets = []
        # lets cache the addrs we ping, so for large number of targets
        # we ping at max the number of ip's in the ip pool.
        ping_client = client
        addr_dict = {}
        for si in si_list:
            alive_targets = []
            addrs = si.get('access', {}).get('ips', [])
            iqn = si.get('access', {}).get('iqn', '')
            if not addrs or not iqn:
                si = si.reload(tenant=self.tenant_path)
                addrs = si.get('access', {}).get('ips', [])
                iqn = si.get('access', {}).get('iqn', '')
            if not addrs:
                raise EnvironmentError("Storage instance has no IPS: %s" %
                                       si['path'])
            if not iqn:
                raise EnvironmentError("Storage instance has no IQN: %s" %
                                       si['path'])
            alive_targets = self._verify_alive_targets(
                access_ips=addrs, ping_client=ping_client, iqn=iqn,
                addr_dict=addr_dict, si=si)
            if len(alive_targets) == 2:
                # Discovery will return 2 targets so no need to add the 2nd one
                alive_targets.pop()
            _targets.extend(alive_targets)
        return _targets

    def _get_volumes_from_si_list(self, si_list):
        return itertools.chain.from_iterable(
            (si['volumes'] for si in si_list))

    def _configure_clients_from_si_list(self, client, si_list):
        assert(len(si_list))
        _targets = self._populate_targets_from_si_list(client, si_list)
        logger.debug(" {} targets are populated".format(len(_targets)))

        redirect_mode = None
        # Are we using iSCSI redirect feature?
        if self._iscsi_redirect is False:
            redirect_mode = None
            redirect_ip = None
        elif self._iscsi_redirect is True:
            redirect_mode = "discovery"
            redirect_ip = self._get_iscsi_redirect_ip()
        else:
            # legacy/deprecated usage as a string
            redirect_mode = self._iscsi_redirect
            redirect_ip = self._get_iscsi_redirect_ip()
        logger.debug("Using iSCSI redirect mode: %s", str(redirect_mode))

        logger.debug("From client {}: [{}], attaching {} st_insts from tbs {}"
                     .format(client.name,
                             self._client_ifaces[client],
                             len(si_list),
                             self._name))
        volumes = self._get_volumes_from_si_list(si_list)
        client.iscsi.attach_all_targets(
            targets=_targets,
            iface_name=self._client_ifaces[client],
            redirect_mode=redirect_mode,
            redirect_ip=redirect_ip)
        if not self._block_io:
            client.filesystem.format_all_volumes(volumes)
            client.filesystem.mount_all_volumes(volumes)
        if self._iscsi_rescan is None or self._iscsi_rescan is True:
            client.rescan_iscsi_bus()
        else:
            logger.debug("[{}] Skip rescan_iscsi_bus() since self."
                         "_iscsi_rescan==False".format(self._name))


    def configure_clients_from_client_si_map(self):
        """
           Configure client with subset of si_list based on client_si_map
        """
        # initialize initiator groups
        self.add_client_initiators_to_storage_instances()

        client_si_map = self.get_client_and_si_mapping()

        _client_attach_funcs = []
        _client_attach_args = []
        success = False
        try:
            for client, si_list in client_si_map.viewitems():
                # this is needed to handle fewer clients than SI's
                if len(si_list) > 0:
                    _client_attach_funcs.append(
                        self._configure_clients_from_si_list)
                    _client_attach_args.append([client, si_list])

            p = Parallel(_client_attach_funcs,
                         args_list=_client_attach_args,
                         max_workers=len(client_si_map.keys()))
            p.run_threads()
            success = True

        finally:
            if not success:
                raise EnvironmentError("iscsi login failed from client {}: [{}]"
                                       .format(client.name,
                                               self._client_ifaces[client]))
            else:
                logger.debug("From client {}: [{}], iSCSI login succeed"
                             .format(client.name,
                                     self._client_ifaces[client]))
                return True

    def _get_iscsi_redirect_ip(self):
        """ Returns the access VIP, used for iSCSI redirect """
        access_vip = self._api.system.network.access_vip.get()
        iscsi_redirect_ip = access_vip['network_paths'][0]['ip']
        return iscsi_redirect_ip

    def configure_clients(self):
        self._populate_targets()
        # initialize initiator groups
        self.add_client_initiators_to_storage_instances()

        # Are we using iSCSI redirect feature?
        if self._iscsi_redirect is False:
            redirect_mode = None
            redirect_ip = None
        elif self._iscsi_redirect is True:
            redirect_mode = "discovery"
            redirect_ip = self._get_iscsi_redirect_ip()
        else:
            # legacy/deprecated usage as a string
            redirect_mode = self._iscsi_redirect
            redirect_ip = self._get_iscsi_redirect_ip()
        logger.debug("Using iSCSI redirect mode: %s", str(redirect_mode))

        self._thread_attach_all_targets(redirect_mode=redirect_mode,
                                        redirect_ip=redirect_ip)

    def _thread_attach_all_targets(self, redirect_mode=None,
                                   redirect_ip=None):
        """Allocating a thread for each client to login."""
        funcs = list()
        kwargs = list()
        for client in self._clients:
            # update all targets with iface names before sending them out
            # we need to get all targets
            funcs.append(client.iscsi.attach_all_targets)
            kwargs.append({
                "targets": self._targets,
                "iface_name": self._client_ifaces[client],
                "redirect_mode": redirect_mode,
                "redirect_ip": redirect_ip})
        Parallel(funcs=funcs, kwargs_list=kwargs,
                 max_workers=len(self._clients)).run_threads()
        for client in self._clients:
            # TODO parallelize this block if needed
            if not self._block_io:
                client.filesystem.format_all_volumes(self.volumes)
                client.filesystem.mount_all_volumes(self.volumes)
            client.rescan_iscsi_bus()

    def _populate_targets(self):
        self._targets = []
        # lets cache the addrs we ping, so for large number of targets
        # we ping at max the number of ip's in the ip pool.

        # TODO[jsp]: this was originally self.clients
        # in /src/qa but I don't know who sets that??
        ping_client = random.choice(self._clients)

        addr_dict = {}
        for si in self.storage_instances:
            alive_targets = []
            addrs = si.get('access', {}).get('ips', [])
            iqn = si.get('access', {}).get('iqn', '')
            if not addrs or not iqn:
                si = si.reload(tenant=self.tenant_path)
                addrs = si.get('access', {}).get('ips', [])
                iqn = si.get('access', {}).get('iqn', '')
            if not addrs:
                raise EnvironmentError("Storage instance has no IPS: %s" %
                                       si['path'])
            if not iqn:
                raise EnvironmentError("Storage instance has no IQN: %s" %
                                       si['path'])
            alive_targets = self._verify_alive_targets(
                access_ips=addrs, ping_client=ping_client, iqn=iqn,
                addr_dict=addr_dict, si=si)
            self._targets.extend(alive_targets)

    def _verify_alive_targets(self, access_ips=None, ping_client=None,
                              iqn=None, addr_dict=None, si=None):
        """
        Helper method specifically verifying the client provided can reach the
        target for the storage instance.
        Args:
            access_ips: List of access IPs for a target
            ping_client: Head node that is going to ping the target
            iqn: Target iqn for storage instance
            si: Storage instance to check (only for failure logging.)
            addr_dict: Dictionary with this structure:
        """
        if not all([access_ips, ping_client, iqn, si]):
            msg = ("All parameters are required to verify targets alive:"
                   " access_ips = {}, ping_client={}, iqn={}, si={},"
                   " addr_dict={}".format(access_ips, ping_client, iqn,
                                          si, pformat(addr_dict)))
            raise ValueError(msg)
        alive_targets = list()
        # allowing for faster responses on success, slower response on failure
        ping_counts = [3, 5, 25]
        # speeding up ping checks
        ping_interval = 0.2
        for ping_time in ping_counts:
            for ip_addr in access_ips:
                if ((addr_dict.get(ip_addr, False) is True
                     or ping_client.networking.ping(ip_addr, count=ping_time,
                                                    interval=ping_interval,
                                                    partial=True))):
                    port = 3260
                    alive_targets.append((ip_addr, port, iqn))
                    addr_dict[ip_addr] = True
            if not alive_targets:
                msg = ("Endpoint addr not reachable from {}: {} with ping"
                       " count of {} on target node: {}".format(
                            ping_client.name, ip_addr, ping_time,
                            si["active_storage_nodes"]))
                logger.warning(msg)
                msg += "Target info:\n{}".format(si)
                logger.debug(msg)
        if not alive_targets:
            msg = ("After attempting these pings intervals: {} on these ips {},"
                   " STILL not able to ping from {} to this"
                   " target, \n{}".format(
                    ping_counts, access_ips, ping_client.name, si))
            raise ValueError(msg)
        return alive_targets


    def cleanup(self):
        """
        Cleans up the cluster and client with configs performed by this class
        """
        logger.debug("Cleanup routine called")
        self.cleanup_clients()
        self.cleanup_cluster()


    def _apply_acl_policy_to_storage_instances(self, init_grp):
        if self._initiator_group:
            for si in self.storage_instances:
                si.acl_policy.initiator_groups.add(init_grp,
                                                   tenant=self.tenant_path)


    def cleanup_cluster(self):
        """
        Cleanup exports and then volumes
        """
        self._api.cleanup()
        self.app_instances = []
        self.storage_instances = []
        self.volumes = []


    def add_client_initiators_to_storage_instances(self):
        if not self._initiator_group:
            self._initiator_group = self._create_initiator_group_for_clients(
                self._clients)
            # apply acl policy to all storage instances
            self._apply_acl_policy_to_storage_instances(self._initiator_group)
            self._poll_storage_instances_ready()
        # 1.0.1 workaround:
        if self._api.system.get()['sw_version'].startswith("1.0.1"):
            time.sleep(10 + (len(self.storage_instances) * 3))


    def _create_temp_api(self):
        """
        Exposing this method so the sub class can override this method
        For example, for tenanttestbedsetup, we need to login as a specific
        user instead of the root.
        """
        return api.sdk_from_cluster(self._cluster)

    def _create_initiator_group_for_clients(self, clients_list):
        initiator_list = []
        # NOTE: initiators presently are not unique across different testbed
        # setups. So we cant delete them without affecting other / parallel
        # tests using them. Therefore we create a new api object and dont
        # call cleanup on them. Only downside is we will leave initiators
        # on the cluster but that is not an issue as it doesnt change.
        temp_api = self._create_temp_api()

        # Checking this here because the function name is ridiculously long.
        support_force = self._cluster.util.is_cluster_version_greater_than_min(
            min_version='3.0.0')

        # The 1.0.1 version of the product does not support EUI64 format
        # initiator names, so if we're configuring a legacy system (probably
        # prior to an upgrade test), configure only standard IQNs:
        allow_eui64 = False

        for client in clients_list:
            client_name = client.name
            iface_name = client.iscsi.get_new_iface(allow_eui64=allow_eui64)
            self._client_ifaces[client] = iface_name
            client_iname = client.iscsi.get_initiator_name(iface_name)
            client_initiator_iqns = [client_iname]

            # For the client's default interface and the newly created
            # interface, we get/create initiator objects on the cluster
            # and store them in initiator_list.
            for client_iqn in client_initiator_iqns:
                initiator = None
                try:
                    # check for already existing initiator
                    initiator = temp_api.initiators.get(
                        client_iqn, tenant=self.tenant_path)
                except ApiNotFoundError:
                    # not found, so create it
                    # But make sure the software version supports all kwargs.
                    initiator_kwargs = {
                        "id": client_iqn,
                        "name": "{}-{}".format(client_name, iface_name)}
                    if support_force:
                        initiator_kwargs['force'] = True

                    try:
                        initiator = temp_api.initiators.create(
                            tenant=self.tenant_path, **initiator_kwargs)
                    except ApiConflictError:
                        # another thread beat us to it
                        initiator = temp_api.initiators.get(
                            client_iqn, tenant=self.tenant_path)
                initiator_list.append(initiator)

        # now we add the initiators to the group
        # do this only if clients are present
        # corner case for setups without clients
        # use original api object so this is cleaned up at the end
        init_grp = None
        if clients_list:
            init_group_kwargs = {"name": name_generator("ig_")}
            if support_force:
                init_group_kwargs['force'] = True
            init_grp = self._api.initiator_groups.create(
                tenant=self.tenant_path, **init_group_kwargs)
            for initiator in initiator_list:
                init_grp.members.add(initiator, tenant=self.tenant_path)
        return init_grp


    def cleanup_clients(self):
        for client in self._clients:
            if not self._block_io:
                client._filesystem.unmount_all_volumes()
            client.iscsi.logout_all_targets()
            client.iscsi.delete_all_targets()

    def get_client_and_si_mapping(self):
        """
        Returns: client and volume mapping to use for qalib.load objects
        """
        with self._lock:
            if not self._client_si_map:
                self._client_si_map = round_robin(self._clients,
                                                  self.storage_instances)

        return self._client_si_map.copy()

    def configure_cluster(self):
        """
        This method will create all volumes, assign them to respective exports
        """
        if self._single_node:
            # TODO - This is not implemented yet. Will be done in subsequent
            # patch
            # self._configure_cluster_with_exports_on_single_node()
            raise NotImplementedError
        else:
            self._default_configure_cluster()

    def _default_configure_cluster(self):
        # First, configure the failure domains.
        self._configure_failure_domains()
        # TODO: Add option to parameterize whether failure domains are created
        # before or after creating app instances.

        # We need to configure app instances.
        # There are two ways to do this:
        # 1. App template
        # 2. Stand alone app instance
        # Given that stand-alone instance and app template are very similar
        # we can create a tempate and spawn multiple instances for default
        # scenarios. Otherwise, we can loop through and create stand alone
        # instances
        for app_temp_container in self._app_templates_container:
            ai = self._create_standalone_ai(app_temp_container)
            self.app_instances.extend(ai)
        # populate attributes
        self._populate_tbs_attributes()
        # see if storage instances are ready
        self._poll_storage_instances_ready()
        # We need to make sure all the volumes in each storage instance
        # is in "available" state.
        self._poll_volumes_ready()

    def _configure_failure_domains(self):
        """
        Configures failure domains based on failure domain container objects.
        """
        if not self.features.failure_domains:
            return

        # Check if there are any failure domains to set.
        if not self._failure_domain_containers:
            return
        else:
            raise NotImplementedError("FDs not supported in this toolkit")


    def _create_standalone_ai(self, app_temp_container):
        app_instance_list = []

        for _ais in range(app_temp_container.count):
            app_instance = self._api.app_instances.create(
                name=name_generator(str(app_temp_container.name)),
                tenant=self.tenant_path)

            si_data = app_temp_container.sis
            # create storage instances
            for cnt in range(si_data.count):
                si = app_instance.storage_instances.create(
                    name=name_generator(str(si_data.name) + str(cnt)),
                    tenant=self.tenant_path)
                for vol_data in si_data.volumes:
                    # Determine the REST parameters
                    create_params = {}
                    create_params['name'] = name_generator(str(vol_data.name))
                    create_params['size'] = int(vol_data.size / GB)
                    create_params['replica_count'] = vol_data.replicas
                    if vol_data.placement_mode:
                        create_params['placement_mode'] = \
                            vol_data.placement_mode
                    if vol_data.placement_policy:
                        create_params['placement_policy'] = {
                                "path":"/placement_mode/"+vol_data.placement_policy}
                    # Send the REST request to create it
                    vol = si.volumes.create(tenant=self.tenant_path,
                                            **create_params)
                    # add vol snapshot policy if present
                    if vol_data.snapshot_policy:
                        vol.snapshot_policies.create(
                            name=name_generator(str(
                                vol_data.snapshot_policy.name)),
                            retention_count=int(
                                vol_data.snapshot_policy.retention_count),
                            interval=vol_data.snapshot_policy.interval)
            # add app snapshot policy at AI level if present
            if app_temp_container.snapshot_policy:
                app_instance.snapshot_policies.create(
                    name=name_generator(str(
                        app_temp_container.snapshot_policy.name)),
                    retention_count=int(
                        app_temp_container.snapshot_policy.
                        retention_count),
                    interval=app_temp_container.snapshot_policy.interval)
            app_instance_list.append(app_instance)
        return [ai.reload(tenant=self.tenant_path) for ai in app_instance_list]


    def _populate_tbs_attributes(self):
        self.app_instances = [ai.reload(tenent=self.tenant_path)
                              for ai in self.app_instances]
        for ai in self.app_instances:
            for si in ai.storage_instances.list(tenant=self.tenant_path):
                self.storage_instances.append(si)
                for vol in si.volumes.list(tenant=self.tenant_path):
                    self.volumes.append(vol)

    def _poll_storage_instances_ready(self):
        """ Ensure self.storage_instances are all available """
        for si in self.storage_instances:
            for result in poll(si.reload,
                               kwargs=({"tenant": self.tenant_path}),
                               timeout=STORAGE_INSTANCE_AVAILABLE_TIMEOUT,
                               interval=3):
                if result.op_state == "available":
                    break
            else:
                msg = "Storage instance:%s is not T_DEPLOYED" % si
                raise EnvironmentError(msg)


    def _poll_volumes_ready(self):
        """ Ensure self.volumes are all available """
        for vol in self.volumes:
            for result in poll(vol.reload,
                               kwargs={"tenant": self.tenant_path},
                               timeout=VOLUME_AVAILABLE_TIMEOUT,
                               interval=5):
                if result.op_state != "unavailable":
                    break
            else:
                raise EnvironmentError(
                    "Volume %s status:%s != available" % (
                        result.uuid, result.op_state))


    @classmethod
    def from_dict(cls, config_dict, equipment, clients=None,
                  required_clients=True, iscsi_redirect=None,
                  client_optimized=None, iscsi_rescan=None,
                  tenant_path=None):
        """
        Returns the testbedsetup object to be used in the test.

        Example config_dict:
        SETUP = {'num_app_instances': 1,
                 'num_storage_instance_per_ai': 1,
                 'volumes_per_si': 2,
                 'volume_size': 10737418240,  # 10GB
                 'placement_mode': hybrid, # hybrid, single_flash, all_flash,
                    # defaults to None, unspecified
                 'num_replicas': 2,  # if not provided randomized between 2,3
                 'snapshot_policy_interval':"15min",
                    # if None, randomize the interval
                 'snapshot_policy_retention': 10, # defaults to 10
                 'snapshot_level': "app_instance"/"volume" # defaults to app
                    instance level.
                 'failure_domains': True # See note about failure domains
                 'failure_domain_cleanup': False # bool: Defaults to False.
                }

        Parameters:
          config_dict = dictionary of setup info
          equipment = qalib.equipment object
          clients = list of clients config needs to be done. Defaults to
            all the clients in the cluster
          required_clients = bool, whether client systems are required
          iscsi_redirect = bool, whether to use iSCSI redirect feature
          client_optimized = bool, whether to login to target from one client
            only
          iscsi_rescan = bool, whether to rescan iscsi bus

        Note:
            The 'failure_domain' key can be mapped to several types of
            variables.

            bool: Enable/disable configuration of failure domains.
                If True, TestBedSetup will choose randomly between 2 or 3
                failure domains.
                If False, TestBedSetup will not change any failure domains.
                This uses the 'truthiness of the variable.  None, 0, {}, will
                evaluate to False
                  e.g.: {'failure_domains': False/True}
            int: Specify number of failure domains to create. This will attempt
                to randomly place nodes into an approximately equal length
                number of failure domains.
                  e.g. {'failure_domains': 2}
            dict: To explicitly set failure domains of each node, create a
                dictionary that has the desired failure domain names as keys,
                and a list of node UUIDs as the value.
                  e.g.: 'failure_domains': {'FD_1': [node1_uuid, node2_uuid],
                                            'FD_2': [node3_uuid]}

            In the case that the config dictionary does not contain a key/value
            pair for 'failure_domains', the default will be
            testrun_params.DEFAULT_FAILURE_DOMAINS.
        """
        config_dict = dict(config_dict)  # shallow copy

        # volume specific keys
        volumes_per_si = int(config_dict.pop("volumes_per_si", 1))
        volume_size = int(config_dict.pop("volume_size", 1 * GB))
        volume_replicas = config_dict.pop("num_replicas", None)
        if volume_replicas is not None:
            volume_replicas = int(volume_replicas)

        # export specific keys
        num_sis = int(config_dict.pop("num_storage_instance_per_ai", 1))
        num_apps = int(config_dict.pop("num_app_instances", 1))
        placement_mode = config_dict.pop("placement_mode", None)
        placement_policy = config_dict.pop("placement_policy", None)

        # failure domain specific keys
        fd_info = config_dict.pop("failure_domains",
                                  testrun_params.DEFAULT_FAILURE_DOMAINS)
        fd_teardown = config_dict.pop("failure_domain_cleanup", None)

        if config_dict:
            msg = "Invalid keys in config dict: "
            msg += ', '.join([str(key) for key in config_dict])
            raise ValueError(msg)

        # since this is just a data container, we can create each object
        # by itself, then link it to the correct object
        # for simplicity start from bottom up
        # NOTE: scenarios addressed by this method will be uniform, i.e.
        # all volumes will be similar etc. Therefore we can get away with
        # shallow copy instead of having a unique object for each
        volumes = [_VolumeDataContainer(
            name="tbs_volume", size=volume_size,
            replicas=volume_replicas,
            placement_mode=placement_mode,
            placement_policy=placement_policy) for _ in xrange(volumes_per_si)]
        storage_instance = _StorageInstanceContainer(
            name="tbs_si", count=num_sis, volume_containers=volumes)
        # NOTE: there is 64 instance limit per template, so if more than 64
        # instances are needed, we need to create more templates
        app_templates = []
        counts = _get_counts_distribution(num_apps)
        for i, cnt in enumerate(counts):
            app_template = _AppDataContainer(
                name="tbs_ai" + unicode(i),
                storage_instance_container=storage_instance,
                count=cnt)
            app_templates.append(app_template)

        failure_domains = None
        if fd_info:
            raise NotImplementedError("Toolkit does not currently support failure domains")

        return cls(equipment, app_templates,
                   clients=clients, required_clients=required_clients,
                   iscsi_redirect=iscsi_redirect,
                   client_optimized=client_optimized,
                   iscsi_rescan=iscsi_rescan,
                   failure_domains=failure_domains,
                   fd_teardown=fd_teardown,
                   tenant_path=tenant_path)


class _VolumeDataContainer(object):

    def __init__(self, name, size=10 * GB, replicas=None,
                 snapshot_policy_container=None, placement_mode=None,
                 placement_policy=None):
        self.name = name
        self.size = size
        if replicas:
            self.replicas = replicas
        else:
            # to be safe for now, select only 2 or 3 as replica count
            self.replicas = randint(2, 3)
        self._uuid = None
        self.performace_policy = None
        self.snapshot_policy = snapshot_policy_container
        self.placement_mode = placement_mode
        self.placement_policy = placement_policy

class _StorageInstanceContainer(object):

    def __init__(self, name, authentication=None, count=1,
                 volume_containers=[]):
        self.name = name
        self.volumes = volume_containers
        self.auth = authentication
        self.count = count


def _get_counts_distribution(num):
    multiple = int(num / 64)
    remainder = num % 64
    cnts = []
    if multiple == 0:
        cnts.append(remainder)
    else:
        cnts = [64] * multiple
        if remainder != 0:
            cnts.append(remainder)
    return cnts


class _AppDataContainer(object):

    def __init__(self, name, count=1, storage_instance_container=[],
                 clients_containers=[], snapshot_policy_container=None):
        self.name = name
        self.sis = storage_instance_container
        self.count = count
        # NOTE: clients_container should be added only if we want to limit
        # where the volume will be discovered on
        # i.e. 2 app instances created, with one mounted on each client
        self.clients = clients_containers
        self.snapshot_policy = snapshot_policy_container


def from_dict(config_dict, equipment, **kwargs):
    """
    Returns a TestBedSetup instance (calls TestBedSetup.from_dict)
    """
    return TestBedSetup.from_dict(config_dict, equipment, **kwargs)
