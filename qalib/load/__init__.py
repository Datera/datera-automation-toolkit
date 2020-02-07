# -*- coding: utf-8 -*-
"""
Workload objects for running IO.

Example: Run IO for 10 minutes on 2 volumes:
    # Create volumes:
    appinst = api.app_instances.create(name="ai1")
    storinst = appinst.storage_instances.create(name="si1")
    vol1 = storinst.volumes_create(name="testvol1",
                                   size=10*1024*1024*1024)
    vol2 = storinst.volumes_create(name="testvol2",
                                   size=10*1024*1024*1024)
    # iSCSI login:
    client.attach_export_config(storinst)
    # Create IO object:
    io = qalib.load.from_client_and_storage_instance(client, storinst,
                                                     tool="fio_comcast")
    # Run IO:
    with io.start():
        time.sleep(600)
"""
import logging
import random

import qalib.load
from . import composite
from . import iospecs
from .fio import FIO

__copyright__ = "Copyright 2020, Datera, Inc."

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


DEFAULT_TOOL = "fio"
S3_RETRY_TOOL = "s3_command_retry"


def _from_iospec(iospec, tool=DEFAULT_TOOL, **kwargs):
    if tool is None:
        tool = DEFAULT_TOOL
    if tool == "fio":
        from qalib.load.fio.default_fio import DefaultFIO
        return DefaultFIO(iospec, **kwargs)
    elif tool == "fio_write_verify":
        from qalib.load.fio.write_verify import FIOWriteVerify
        return FIOWriteVerify(iospec, **kwargs)
    else:
        raise ValueError("Unknown load generator: " + repr(tool))


def _is_si_object(storage_instance):
    """
    Helper method for determining if a storage instance is object.
    Args:
        storage_instance:

    Returns: (Bool) True if object, False if not.
    """
    si_type = storage_instance.get("service_configuration", None)
    if si_type is None:
        # object not supported on storage instance
        return False
    elif si_type == "object":
        return True
    else:
        return False


def from_client_and_volume_list(client,
                                volume_list, tool=DEFAULT_TOOL, **kwargs):
    """
    Creates an IO object
    Parameters:
      client (qalib.client.Client)
      volume_list (list) - qalib.api.ApiResource volumes to do I/O to
    The volumes must belong to exports which this client has logged into
    """
    iospec = iospecs.from_client_and_volume_list(client, volume_list)
    return _from_iospec(iospec, tool=tool, **kwargs)


def from_client_and_storage_instance(client,
                                     storage_instance,
                                     tool=DEFAULT_TOOL,
                                     **kwargs):
    """
    Creates an IO object
    Parameters:
      client (qalib.client.Client)
      storage_instance (qalib.api.api2.Entity)
    All volumes in the storage_instance will be used
    """
    return from_client_and_storage_instance_list(client, [storage_instance],
                                                 tool=tool, **kwargs)


def from_client_and_storage_instance_list(client,
                                          storage_instance_list,
                                          tool=DEFAULT_TOOL,
                                          tenant_path="/root",
                                          **kwargs):
    """
    Creates an IO object
    Parameters:
      client (qalib.client.Client)
      storage_instance_list ([qalib.api.api2.Entity])
    All volumes in the storage_instance_list will be used
    """
    volume_list = []
    for storage_instance in storage_instance_list:
        volume_list.extend(storage_instance.volumes.list(
            tenant=tenant_path))
    volume_uuid_list = [volume['uuid'] for volume in volume_list]
    return from_client_and_volume_uuid_list(client,
                                            volume_uuid_list,
                                            tool=tool, **kwargs)


def from_client_and_volume_uuid_list(client,
                                     volume_uuid_list,
                                     tool=DEFAULT_TOOL,
                                     **kwargs):
    """
    Creates an IO object
    Parameters:
      client (qalib.client.Client)
      volume_uuid (str)
    The client must be logged into this volume's export
    """
    if tool == "fs_io":
        iospec = iospecs.from_client_and_fs_io_vol_uuid_list(
            client, volume_uuid_list)
        return _from_iospec(iospec, tool=tool, **kwargs)
    else:
        iospec = iospecs.from_client_and_volume_uuid_list(
            client, volume_uuid_list)
        return _from_iospec(iospec, tool=tool, **kwargs)


def composite_from_testbed_setup(tbs, tool=DEFAULT_TOOL, **kwargs):
    """
    Returns an IoComposite object which controls the given list of IO
    instances, as if they were a single IO instance.
    """
    # Adding a default value of 1 for iodepth if it is not specified on vm.
    # Otherwise, it defaults to 16, which is too aggressive for vm's.
    if not tbs.is_on_hardware():
        if 'iodepth' not in kwargs:
            kwargs['iodepth'] = 1
    client_si_map = tbs.get_client_and_si_mapping()
    # prevent overloading fio by splitting up fio into multiple processes
    max_sis_per_fio = 16
    io_list = list()
    for client, storage_instances in client_si_map.items():
        client_si_list = list()
        for instance in storage_instances:
            if _is_si_object(storage_instance=instance):
                object_kwargs = kwargs.copy()
                if tool == S3_RETRY_TOOL:
                    object_kwargs['client_obj'] = client
                    object_kwargs['storage_instance'] = instance
                every_ip = False
                if "every_ip" in object_kwargs:
                    every_ip = object_kwargs.pop("every_ip")
                for access_details in tbs.object_store_details(
                        instance, every_ip=every_ip):
                    object_kwargs.update(access_details)
                    io_list.append(from_client_and_volume_list(
                        client, instance.volumes.list(),
                        tool=tool, **object_kwargs))
            else:
                client_si_list.append(instance)
        if client_si_list:
            # splitting into smaller objects
            io_chunks = [client_si_list[i:i+max_sis_per_fio] for i in range(
                0, len(client_si_list), max_sis_per_fio)]
            for io_chunk in io_chunks:
                if hasattr(tbs, 'tenant_path'):
                    kwargs["tenant_path"] = tbs.tenant_path
                io_list.append(from_client_and_storage_instance_list(
                    client, io_chunk, tool=tool, **kwargs))
    random.shuffle(io_list)
    return composite.from_io_list(io_list)


def from_tbs(*args, **kwargs):
    """ Short-cut for composite_from_testbed_setup() """
    return composite_from_testbed_setup(*args, **kwargs)
