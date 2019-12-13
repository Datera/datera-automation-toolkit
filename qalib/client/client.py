# -*- coding: utf-8 -*-
"""
Provides the Client class
"""
from __future__ import (unicode_literals, print_function, division)

import re
from os import getpid
import uuid
import functools
import logging
import pkgutil
from pprint import pformat

import qalib.corelibs.system
from qalib.client.multipath import Multipath
from qalib.client.iscsi import ISCSID_CONF_TEMPLATE, ISCSIInitiator
from qalib.corelibs.system import _UDEV_SEMIS, _UDEV_TIMEOUT

__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

# irq_affinity script location
_IRQ_AFFINITY_SCRIPT = "/usr/bin/irq_affinity.sh"

# Change this filename every time the client setup code changes:
_CLIENT_CONFIGURED_FLAG = "/root/.qa_client_configured_1.13"

# iSCSI initiator config file
_ISCSI_INIT_CONFIG_FILE = "/etc/iscsi/iscsid.conf"
# iSCSI service config file
_ISCSI_SERVICE_FILE = "/usr/lib/systemd/system/iscsi.service"
# iSCSI initiator connection timeout
_ISCSI_REPLACEMENT_TIMEOUT = 120
_ISCSI_LOGIN_TIMEOUT = 120

# iSCSI initiator connection timeout
_ISCSI_REPLACEMENT_TIMEOUT = 120

_SCRIPT_FOR_MPATH = '/sbin/fetch_device_serial_no.sh'
_MPATH_SCRIPT_DATA = ("#!/bin/sh\n"
                      "# Script used to fetched the Unit serial number of the"
                      " given device /dev/sd* or /dev/dm-*\n"
                      "# This script must be present at location.\n"
                      "/usr/bin/sg_inq /dev/$1 | /bin/grep '"
                      " Unit serial number:' | /usr/bin/awk '{print $4}'\n")
_UDEV_RULE_FILE = "/etc/udev/rules.d/99-iscsi-luns.rules"
_UDEV_RULE = (
    '# Create symlink in "/dev/disk/by-uuid" folder for a volume.'
    ' Symlink name is same as UUID of the volume.\n'
    '# Rule for the devices with VENDOR as DATERA\n'
    'SUBSYSTEM=="block", ENV{DEVTYPE}=="disk",'
    ' ENV{ID_VENDOR}=="DATERA", '
    ' SYMLINK+="disk/by-uuid/$env{ID_SCSI_SERIAL}"\n'
    'SUBSYSTEM=="block", ENV{DEVTYPE}=="disk", '
    'ENV{ID_VENDOR}=="DATERA", '
    'SYMLINK+="disk/by-uuid/$env{SCSI_IDENT_SERIAL}"\n'
    '# Rule for the devices with VENDOR as LIO-ORG\n'
    'SUBSYSTEM=="block", ENV{DEVTYPE}=="disk",'
    ' ENV{ID_VENDOR}=="LIO-ORG",'
    ' SYMLINK+="disk/by-uuid/$env{ID_SCSI_SERIAL}"\n'
    'SUBSYSTEM=="block", ENV{DEVTYPE}=="disk", ENV{ID_VENDOR}=="LIO-ORG", '
    'SYMLINK+="disk/by-uuid/$env{SCSI_IDENT_SERIAL}"\n'
    '# RULE FOR MULTIPATHING. If multipathing is enabled then symlink'
    ' points to multipath device (/dev/dm-*).\n'
    'KERNEL=="dm-[0-9]*", SUBSYSTEM=="block", ENV{DEVTYPE}=="disk",'
    ' PROGRAM=="/bin/sh -c \'' + _SCRIPT_FOR_MPATH + ' %k\'",'
    ' NAME="%k", SYMLINK+="disk/by-uuid/%c"\n'
    '# Automatically remove device symlink on remove event.\n'
    'KERNEL=="dm-[0-9]*", ACTION=="remove", '
    'SUBSYSTEM=="block",'
    ' ENV{DEVTYPE}=="disk", PROGRAM=="/bin/sh -c'
    ' \'/bin/rm /dev/disk/by-uuid/$env{ID_SCSI_SERIAL}\'"\n')

def _setup_required(method):
    """
    Decorator to mark Client methods as requiring a configured client
    """
    @functools.wraps(method)
    def wrapper_method(self, *args, **kwargs):
        """ Call the original method, calling setup() first as needed """
        if not self._is_configured:  # pylint: disable=protected-access
            self.setup()
            self._is_configured = True  # pylint: disable=protected-access
        return method(self, *args, **kwargs)
    return wrapper_method


class Client(qalib.corelibs.system.System):  # pylint: disable=too-many-public-methods

    """
    Controls a remote client system.
    Currently, only Linux systems are supported.
    """
    def __init__(self, *args, **kwargs):
        """
        Call the superclass init, then prepare this client for action
        """
        super(Client, self).__init__(*args, **kwargs)
        self._is_configured = False
        self._iscsi = None
        self._filesystem = None
        self._cluster = None
        self.block_io = True
        self._mpath = None
        self._irq_set = None

    @property
    @_setup_required
    def cluster(self):
        if not self._cluster:
            raise EnvironmentError(
                "Client object does not have cluster attached")
        return self._cluster

    @property
    @_setup_required
    def iscsi(self):
        """ qalib.client.iscsi.ISCSIInitiator """
        if not self._iscsi:
            self._iscsi = ISCSIInitiator(self)
        return self._iscsi

    @property
    @_setup_required
    def mpath(self):
        """ qalib.client.multipath.Multipath """
        if not self._mpath:
            if self.rhel_version_starts_with("7") or not self.on_hardware():
                self._mpath = Multipath(self, os_version="rhel_7")
            else:
                self._mpath = Multipath(self)
        return self._mpath

    def cleanup_client(self):
        """
        This method cleans up the client using the state maintained in each
        module.
        For all calsoft related tests, please use this.
        """
        if not self.block_io:
            # clean up only if object exists
            if self._filesystem:
                self.filesystem.unmount_all_volumes()
        if self._iscsi:
            self.iscsi.logout_all_targets()
            self.iscsi.delete_all_targets()

    def setup(self):
        """ Prepare the client system """
        if self._is_configured and self._irq_set:
            return
        if self.path_exists(_CLIENT_CONFIGURED_FLAG):
            self._is_configured = True
        else:
            self._install_client_packages()
            self._prepare_udev_rules()

        self._configure_irq_affinity()

        with _UDEV_SEMIS[self.name]:
            self.run_cmd("udevadm control --reload")
        self.increase_kernel_aio_value()
        # Ensure multipath is enabled
        # multipath enablement has to come after touch _CLIENT_CONFIGURED_FLAG
        # If not,
        # because that the enable multipath method also runs the
        # client setup method, we will have a infinite loop
        self.mpath.enable_multipath()

    def _install_client_packages(self):
        """Installs required client packages"""
        # need to remove /etc/iscsi/iscsid.conf for install to be quite
        if self.path_exists("/etc/iscsi/iscsid.conf"):
            self.remove_filepath("/etc/iscsi/iscsid.conf")
        self._logger.debug("Client " + self.name + " needs configuration")
        # Install required packages:
        if self.is_centos():

            cmd = "for rpm in" + \
                " iscsi-initiator-utils sg3_utils sysstat fio lsscsi" + \
                " python-pip" + \
                " curl" + \
                " ; do yum install -y $rpm ; done"
        else:
            raise RuntimeError("Clients must be CentOS systems.")
        self.run_cmd(cmd)

        # if not self.path_exists("/usr/bin/apt-get"):
        if not self.is_ubuntu():
            # need to install iscsi.service configuration
            self._install_iscsi_service_conf()
        else:
            # Start iSCSI
            cmd = "/etc/init.d/open-iscsi start"
            self.run_cmd(cmd)

        # Ensure that iSCSI initiator is set with right timeout value.
        self._set_iscsi_timeout()

        # Install s3cmd with pip because Ubuntu repositories are the worst,
        # i.e. they don't have the newest versions of s3cmd.
        self.run_cmd("pip install -U pip")
        self.run_cmd("pip install -U s3cmd || pip install s3cmd")

        # Set timezone to UTC:
        cmd = ("ln -s -f ../usr/share/zoneinfo/Etc/UTC /etc/localtime ; "
               "timedatectl set-timezone UTC")
        self.run_cmd(cmd)

        # Mark client as already set up
        self.run_cmd("touch " + _CLIENT_CONFIGURED_FLAG)
        self._is_configured = True

    def _prepare_udev_rules(self):
        """Sets up udev rules"""
        # Prepare udev rule
        tmpsuffix = "." + str(getpid()) + ".tmp"
        script_for_mpath_tmp = _SCRIPT_FOR_MPATH + tmpsuffix
        with self.file_open(script_for_mpath_tmp, 'w') as tmpscriptf:
            tmpscriptf.write(_MPATH_SCRIPT_DATA)
        self.run_cmd("chmod 755 " + script_for_mpath_tmp)
        self.run_cmd("mv -f -- " + script_for_mpath_tmp + " " +
                     _SCRIPT_FOR_MPATH)
        rulefile_tmp = _UDEV_RULE_FILE + tmpsuffix
        with self.file_open(rulefile_tmp, 'w') as udevf:
            udevf.write(_UDEV_RULE)
        self.run_cmd("mv -f -- " + rulefile_tmp + " " + _UDEV_RULE_FILE)

    def _install_iscsi_service_conf(self):
        """
        Installs the file for the iscsi service here:
        /usr/lib/systemd/system/iscsi.service
        """
        local_path = "assets/iscsi.service"
        remote_path = _ISCSI_SERVICE_FILE

        # Upload it to a temporary location on the client:
        random_str = unicode(uuid.uuid1()).replace('-', '')
        remote_tmppath = ".".join((remote_path, random_str))
        data = pkgutil.get_data(__name__, local_path)
        if not data:
            raise ValueError("Could not load package data %s" % local_path)
        with self.file_open(remote_tmppath, 'w') as tmpf:
            tmpf.write(data)
        self.run_cmd_check("chmod +x " + remote_tmppath)
        # Move it to the permanent location:
        self.run_cmd_check("mv -f -- " + remote_tmppath + " " + remote_path)
        logging.debug("Installed iscsi_service on client %s at %s", self.name,
                      remote_path)
        return remote_path

    def _install_irq_affinity_script(self):
        """Installs the irq affinity script on the client."""
        local_path = "assets/irq_affinity.sh"
        remote_path = _IRQ_AFFINITY_SCRIPT

        # Upload it to a temporary location on the client:
        random_str = unicode(uuid.uuid1()).replace('-', '')
        remote_tmppath = ".".join((remote_path, random_str))
        data = pkgutil.get_data(__name__, local_path)
        if not data:
            raise ValueError("Could not load package data %s" % local_path)
        with self.file_open(remote_tmppath, 'w') as tmpf:
            tmpf.write(data)
        self.run_cmd_check("chmod +x " + remote_tmppath)
        # Move it to the permanent location:
        self.run_cmd_check("mv -f -- " + remote_tmppath + " " + remote_path)
        logging.debug("Installed irq_affinity on client %s at %s", self.name,
                      remote_path)
        return remote_path

    def _configure_irq_affinity(self):
        """Method for checking and setting up irq affitinity"""
        if not self.path_exists(_IRQ_AFFINITY_SCRIPT):
            self._install_irq_affinity_script()
        self.run_cmd(_IRQ_AFFINITY_SCRIPT)
        self._irq_set = True

    ########################################

    @_setup_required
    def get_device_for_volume_uuid(self, uuid):
        """
        For a given volume uuid, returns one client device file which
        correspond to the volume, e.g. "/dev/sdc"
        Parameter:
          uuid (str) - volume uuid
        If there is no device file, returns None
        """
        # If there's a /dev/disk/by-uuid link for this volume, return that:
        by_uuid_path = "/dev/disk/by-uuid/" + str(uuid)
        cmd = ("test -e %s || "
               "( udevadm trigger ; udevadm settle ) >/dev/null 2>&1 ; "
               " test -e %s && readlink -e %s || true") % (by_uuid_path,
                                                           by_uuid_path,
                                                           by_uuid_path)
        with _UDEV_SEMIS[self.name]:
            out = self.run_cmd_check(cmd)
        out = out.strip()
        if out:
            return out
        else:
            self._logger.debug("Client %s no device for volume %s",
                               self.name, uuid)
            return None

    ########################################

    def rescan_iscsi_bus(self):
        # Re-scan SCSI bus before retrying...
        self._logger.debug("Re-scan client SCSI devices...")
        cmd = "for d in /sys/class/scsi_host/* ; do"
        cmd += " [ -f ${d}/scan ] && echo \"- - -\" > ${d}/scan ; done ; "
        cmd += "udevadm settle ; udevadm trigger ; udevadm settle"
        cmd += " ; multipath -v2"
        with _UDEV_SEMIS[self.name]:
            self.run_cmd(cmd)

    ########################################

    def increase_kernel_aio_value(self):
        """
        This will increase the value for kernel async io to handle
        parallel io requests to 256KB
        """
        path = "/proc/sys/fs/aio-max-nr"
        try:
            cmd = "echo 262144 > " + path
            self.run_cmd_check(cmd)
            self._logger.debug("Increased max aio count on %s" % self.name)
        except Exception as ex:
            msg = ("Failed to increase kernel aio.\n{}".format(str(ex)))
            self._logger.exception(msg)

    @_setup_required
    def unmount(self, mountpoint):
        """
        Unmounts a mountpoint
        Parameter:
          mountpoint (str)
        """
        self.filesystem.unmount(mountpoint)

    def cleanup(self):
        """ Do not call this; call cleanup_client() instead """
        self._logger.error(
            "Deprecated method called! Change to: force_cleanup_all()")
        return self.force_cleanup_all()

    @_setup_required
    def force_cleanup_all(self):
        """
        Forcible iscsi client cleanup

        Warning: tests should not normally use this method!!!

        This is a destructive operation which will prevent tests from
        running in parallel, etc.
        """
        # NOTE / TODO - This method does a wholesale clean up as opposed to
        # going by state.  This is disruptive operation for any other test
        # utilizing the same client.
        # Calsoft code should use cleanup_client till the time this renamed
        # and relevant test cases updated

        # TODO :
        # 1) If host is on iSCSI LUN (Boot Lun) then session session reset
        #    will bringdown the host. This method has to pick stale entries
        #    wisely and stash the same
        # 2) Device and associated Lun tuple to delete the LUN wisely
        # 3) List of empty exports on server to remove the same.
        # 4) Build a dictionary of work done, transcript of cleanedup.
        # 5) echo 1 > /sys/class/scsi_host/host/device/target::/:::/delete
        #    to delete stained entries
        #
        # Current workflow,
        # *  On Server ( pls. refer clusterutil->cleanup())
        # *    Remove LUN(s)/volem(s) on cluster
        # *    Remove Exports on cluster
        # *  On Host (this method),
        # *    Run iscsiadm --m node -u : Logout on all iSCSI sessions
        # *    Remove all node entries from '/etc/iscsi/nodes'
        # *    Run iSCSI discovery again to ensure that no stale
        # *      target portal entries

        # cleanup tracked state, first:
        self.cleanup_client()

        # TODO: move this code into proper place in qalib.io:
        cmd = ' ; '.join(("killall fio",
                          "killall fio-2.1.13",
                          "killall fio-2.1.14",
                          "killall fio-2.2.8-ppc",
                          "killall fio-2.2.8"))
        cmd += " ; rm -f /var/tmp/fio.*.out ; rm -f /var/tmp/fio.*.cfg"
        cmd += " ; rm -f /var/tmp/IO.*.out"
        _status, out = self.run_cmd(cmd)

        # Unmount all mounted volumes:
        self.filesystem.force_cleanup_all()

        # Remove all target portal entries
        self.iscsi.force_delete_all()

        # ensure that there are no stale entries left on client
        cmd = "iscsiadm --mode node -R"
        _status, out = self.run_cmd(cmd)
        MSTR = re.compile(r'No session found')
        match = re.search(MSTR, out)
        if match:
            self._logger.debug(
                self.name +
                ": cleanup: iSCSI initiator is in clean state")
        else:
            self._logger.debug(
                self.name +
                ": cleanup: FAILED to remove all stale entries.")

        # Clean up a pathological case where somebody (probably running
        # I/O to a device which has been logged out) creates a regular
        # file with a device file name; this causes all kinds of mayhem.
        cmd = ('for dev in /dev/sd* /dev/mpath* /dev/dm-* ; do'
               ' if [ -f "$dev" ] ; then'
               ' ls -ld -- "$dev" ; rm -f -- "$dev" ;'
               ' fi'
               ' ; done')
        self.run_cmd(cmd)

        # Kick multi-pathing:
        cmd = "service multipath-tools restart ; "  # Ubuntu
        cmd += "service multipathd restart && sleep 6 ; "  # RedHat
        cmd += "multipath -F ; multipath -v2 ; multipath -ll"  # flush/re-scan
        self.run_cmd(cmd)

        # Make sure udev isn't broken:
        with _UDEV_SEMIS[self.name]:
            self.run_cmd_check("udevadm trigger && udevadm settle "
                               "--timeout {}".format(_UDEV_TIMEOUT))

        return

    def is_rhel(self):
        """Looks at /etc/redhat-release to see if this is rhel"""
        if self.path_isfile("/etc/redhat-release"):
            return True
        else:
            return False

    def rhel_version_starts_with(self, major_version="7"):
        """If is_rhel(), looks at /etc/redhat-release for 7.x.x"""
        if self.is_rhel():
            release_file_path = "/etc/redhat-release"
            version_regex = ".* {}.\d+.*".format(major_version)
            if self.path_isfile(release_file_path):
                release_info = self.file_open(release_file_path,
                                              'r').readlines()
                for line in release_info:

                    if re.search(version_regex, line):
                        return True
        return False

    def is_datera_os(self):
        """Looks for DaterOS in /etc/redhat-release"""
        if self.is_rhel():
            release_file_path = "/etc/redhat-release"
            if self.path_isfile(release_file_path):
                release_info = self.file_open(release_file_path,
                                              'r').readlines()
                if re.search("DaterOS", release_info):
                    return True
        return False

    def is_ubuntu(self):
        """Looks for ubuntu in /etc/os-release"""
        release_file_path = "/etc/os-release"
        if self.path_isfile(release_file_path):
            release_info = self.file_open(release_file_path, 'r').readlines()
            for line in release_info:
                if "ubuntu" in line.lower():
                    return True
        else:
            return False

    def is_centos(self):
        """
        Check if os is centos

        """
        ret, out = self.run_cmd("lsb_release -a")
        if ret == 0:
            if "CentOS" in out:
                return True
        return False

    def on_hardware(self):
        """
        VM *****************************************
        Architecture:          x86_64
        CPU op-mode(s):        32-bit, 64-bit
        Byte Order:            Little Endian
        CPU(s):                2
        On-line CPU(s) list:   0,1
        Thread(s) per core:    1
        Core(s) per socket:    1
        Socket(s):             2
        NUMA node(s):          1
        Vendor ID:             GenuineIntel
        CPU family:            6
        Model:                 6
        Model name:            QEMU Virtual CPU version 2.5+
        Stepping:              3
        CPU MHz:               2133.254
        BogoMIPS:              4266.50
        Hypervisor vendor:     KVM
        Virtualization type:   full
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              4096K
        NUMA node0 CPU(s):     0,1
        HW *****************************************
        Architecture:          x86_64
        CPU op-mode(s):        32-bit, 64-bit
        Byte Order:            Little Endian
        CPU(s):                12
        On-line CPU(s) list:   0-11
        Thread(s) per core:    1
        Core(s) per socket:    6
        Socket(s):             2
        NUMA node(s):          2
        Vendor ID:             GenuineIntel
        CPU family:            6
        Model:                 44
        Stepping:              2
        CPU MHz:               1600.000
        BogoMIPS:              4533.51
        Virtualization:        VT-x
        L1d cache:             32K
        L1i cache:             32K
        L2 cache:              256K
        L3 cache:              12288K
        NUMA node0 CPU(s):     0-5
        NUMA node1 CPU(s):     6-11
        """
        cpu = self.util.get_lscpu()
        for cpu_key in ["model name", "Model name"]:
            model = cpu.get(cpu_key)
            if model is not None:
                # found the model
                if "QEMU" in model:
                    # model found and on QEMU, on VM
                    return False
                else:
                    # model found, and not QEMU, on hardware
                    return True
        # if we made it here, it means we didn't find the cpu model number
        msg = ("Failed to find cpu model information for determining if client"
               " is on hardware of VM, client = {} expecting 'model name'"
               " or 'Model name', assuming this is hardware output from "
               "lscpu():\n{}".format(
                self.name, pformat(cpu)))
        log.debug(msg)
        return False

    def force_log_rotation(self):
        """
        Force log rotation on the client
        """
        if not self.path_exists("/etc/logrotate.conf"):
            self._logger.exception("Client force log rotation not supported %s"
                                   % self.name)
        rotation_cmd = "/usr/sbin/logrotate --force /etc/logrotate.conf"
        # force log rotation now
        status, err = self.run_cmd(rotation_cmd)
        if status != 0:
            self._logger.warn("Client force log rotation failed %s"
                              % self.name)

    def _set_iscsi_timeout(self, os="Linux"):
        """
        Setup the iscsi timeout by echoing the file to the correct
        location or sed the exsiting file
        """
        cmd = None
        if os != "Linux":
            raise NotImplementedError(
                "Currently only Linux client is supported")
        if self.path_exists("/etc/iscsi/iscsid.conf"):
            # Why sed is used here?
            # In pythonic way, it will be bit slow since it requires 2
            # file objects and read/write sequence.
            cmd = "sed -i \'/node.session.timeo.replacement_timeout/c\\"
            cmd += "node.session.timeo.replacement_timeout = " + \
                str(_ISCSI_REPLACEMENT_TIMEOUT) + "\'"
            cmd += " " + _ISCSI_INIT_CONFIG_FILE + ";"
            cmd += "sed -i \'/login_timeout/c\\"
            cmd += "node.conn[0].timeo.login_timeout = " + \
                str(_ISCSI_LOGIN_TIMEOUT) + "\'"
            cmd += " " + _ISCSI_INIT_CONFIG_FILE
            self._logger.debug("Set iscsi replacement timeout to {}".format(
                _ISCSI_REPLACEMENT_TIMEOUT))
            self.run_cmd_check(cmd)
        else:
            # The file get deleted and didn't get regenerate
            # Echo the content to the correct location
            with self.file_open("/etc/iscsi/iscsid.conf", 'w') as f:
                f.write(ISCSID_CONF_TEMPLATE)


def from_hostname(hostname, username=None, password=None):
    """
    For debugging only.  Production code should use from_equipment().
    Parameter:
      hostname (str)
      username (str) - "root"
      password (str)
    """
    return Client.from_hostname(hostname, username=username, password=password)


def list_from_equipment(equipment, osname=None, required=True):
    """
    Returns a list of Client objects
    Parameter:
      equipment (qalib.corelibs.equipmentprovider.EquipmentProvider)
    Optional parameter:
      os (str) - e.g. "Linux"
    Raises qalib.corelibs.equipmentprovider.EquipmentNotFoundError if no
    clients could be found.
    """
    # On the client._cluster assignment below:
    # NOTE/TODO - presently this works only with one client! Since
    # each instantiation of a clusterutil object maintains its own state.
    # Once equipmentprovider.cluster starts loading cluster object, we can
    # use that one object and assign it to multiple clients. For now letting
    # it be, and instantiating new cluster objects for each client
    cluster = equipment.get_cluster(required=False)
    if cluster:
        # cluster object is re-used across test runs. Since it might have
        # some state of previous runs, creating a fresh copy
        cluster = cluster.copy()
    clientobj_list = list()
    for clientequipment in equipment.get_client_list(required=required, osname=osname):
        connection = clientequipment.get_connection()
        client = Client.from_connection(connection)
        # update clients with cluster object from equipment
        client._cluster = cluster  # pylint: disable=protected-access
        clientobj_list.append(client)
    return clientobj_list


def from_equipment(equipment, osname=None, required=True):
    """
    Returns a Client object
    Parameter:
      equipment (qalib.corelibs.equipmentprovider.EquipmentProvider)
    Optional parameter:
      os (str) - e.g. "Linux"
    Raises qalib.corelibs.equipmentprovider.EquipmentNotFoundError if no
    client could be found.
    """
    client_list = list_from_equipment(equipment, osname=osname, required=required)
    if not client_list:
        return None
    return client_list[-1]
