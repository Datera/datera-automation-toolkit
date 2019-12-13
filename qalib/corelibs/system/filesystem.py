# -*- coding: utf-8 -*-
"""
Provides interface to perform file system related operations
"""
from __future__ import (unicode_literals, print_function, division)
from qalib.qabase.constants import POSIXCLIENT_QA_MOUNT_PREFIX

__copyright__ = "Copyright 2020, Datera, Inc."


class FileSystem(object):
    """
    Provides file system related libraries
    """

    def __init__(self, system):
        """
        Creates a file system object for system type
        !! Should not be called directly !
        This should be accessible via systems.filesystem
        property created
        """
        self.system = system
        self.mount_points = []

    @property
    def mnt_prefix(self):
        """ Top-level directory under which filesystems are mounted """
        return POSIXCLIENT_QA_MOUNT_PREFIX

    def force_cleanup_all(self):
        """
        Forcibly unmount all volumes from the system

        WARNING: this operation is destructive!!

        Test cases should not call this.
        """
        self.mount_points = []
        cmd = "for d in `mount | awk '{print $3}' |"
        cmd += " grep ^" + self.mnt_prefix + "`"
        cmd += ' ; do umount -lf "$d" ; rmdir "$d" ; done'
        self.system.run_cmd(cmd)

    def unmount(self, mountpoint):
        """
        Unmounts filesystem. Raises exception on failure

        Parameter:
          mountpoint (str) - mountpoint to unmount
        """
        # TODO[jsp]: this can't be called on storage nodes,
        # but is anything stopping us from doing just that?
        # TODO: accept either device file or mount point
        cmd_str = "umount -lf " + mountpoint
        self.system.run_cmd_check(cmd_str)
        if mountpoint in self.mount_points:
            self.mount_points.remove(mountpoint)
        cmd_str = "[ -d " + mountpoint + " ] && rmdir " + mountpoint
        self.system.run_cmd(cmd_str)

    def unmount_all_volumes(self):
        """
        Unmounts all volumes which were mounted by this system instance
        """
        for mount in self.mount_points[:]:
            self.unmount(mount)
