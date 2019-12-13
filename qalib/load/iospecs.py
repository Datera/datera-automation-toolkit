"""
Provides IOSpec objects, for passing I/O target data to load generators
"""

__copyright__ = "Copyright 2020, Datera, Inc."


class IOSpec(object):
    """ Base class for IOSpec objects """

    def __init__(self):
        self._blockdev_list = None


class VolumeUUIDIOSpec(IOSpec):
    """ A client system and volume UUIDs """

    def __init__(self, client, volume_uuid_list):
        self._client = client
        self._volume_uuid_list = volume_uuid_list
        if not self._volume_uuid_list:
            raise ValueError("No IO volume UUIDs specified")

    def get_client(self):
        return self._client

    def get_blockdev_list(self):
        """
        Note that if the client does an iSCSI logout then login, the device
        files returned by this object may change
        """
        # TODO: when called repeatedly, log a message if the device
        #       files change.  That's OK, if intended, but might as well
        #       draw attention to it.
        blockdev_list = list()
        for volume_uuid in self._volume_uuid_list:
            blockdev = self._client.get_device_for_volume_uuid(volume_uuid)
            if blockdev is None:
                msg = "Client %s no dev for vol %s" % (self._client.name,
                                                       volume_uuid)
                raise ValueError(msg)
            blockdev_list.append(blockdev)
        return blockdev_list



class VolumeIOSpec(IOSpec):
    """ A client system and volumes """

    def __init__(self, client, volume_list):
        self._client = client
        self._volume_list = volume_list
        if not self._volume_list:
            raise ValueError("No IO volumes specified")


class FSUUIDIOSpec(IOSpec):
    """ A client system and File System UUID's """

    def __init__(self, client, volume_uuid_list):
        self._client = client
        self._volume_uuid_list = volume_uuid_list
        if not self._volume_uuid_list:
            raise ValueError("No IO volume UUIDs specified")


def from_client_and_volume_uuid_list(client, volume_uuid_list):
    """ Returns an IOSpec instance """
    return VolumeUUIDIOSpec(client, volume_uuid_list)


def from_client_and_volume_list(client, volume_list):
    """ Returns an IOSpec instance """
    return VolumeIOSpec(client, volume_list)


def from_client_and_fs_io_vol_uuid_list(client, volume_uuid_list):
    """ Returns an IOSpec instance """
    return FSUUIDIOSpec(client, volume_uuid_list)
