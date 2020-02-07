# -*- coding: utf-8 -*-
"""
Provides interface to perform iscsiadm related operations
"""
from __future__ import (unicode_literals, print_function, division)

from collections import namedtuple
import logging
import pipes
import random
import re
import string
import time

from qalib.qabase.polling import poll
import qalib.qabase.namegenerator as namegenerator
from qalib.qabase.threading import Parallel
from qalib.corelibs.system import (_UDEV_SEMIS, _UDEV_TIMEOUT, _ISCSIADM_SEMIS,
                                   DISCOVERED_TARGETS, ISCSI_LOGGEDIN_TARGETS)

__copyright__ = "Copyright 2020, Datera, Inc."

_DELETE_RETRY_COUNT = 12
_DISCOVERY_RETRY_COUNT = 5
_DISCOVERY_TIMEOUT = 100
_ISCSIADM_RETRY_INTERVAL = 5
_LOGOUT_RETRY_COUNT = 12

MAX_ISCSI_WORKER = 10
ISCSID_CONF_TEMPLATE = \
    """
iscsid.startup = /usr/sbin/iscsid
node.startup = manual
node.leading_login = No
#node.session.auth.authmethod = CHAP
#node.session.auth.username = username
#node.session.auth.password = password
#node.session.auth.username_in = username_in
#node.session.auth.password_in = password_in
#discovery.sendtargets.auth.authmethod = CHAP
#discovery.sendtargets.auth.username = username
#discovery.sendtargets.auth.password = password
#discovery.sendtargets.auth.username_in = username_in
#discovery.sendtargets.auth.password_in = password_in
node.session.timeo.replacement_timeout = 120
node.conn[0].timeo.login_timeout = 120
node.conn[0].timeo.logout_timeout = 15
node.conn[0].timeo.noop_out_interval = 5
node.conn[0].timeo.noop_out_timeout = 5
node.session.err_timeo.abort_timeout = 15
node.session.err_timeo.lu_reset_timeout = 30
node.session.err_timeo.tgt_reset_timeout = 30
node.conn[0].timeo.login_timeout = 120
node.session.initial_login_retry_max = 8
node.session.cmds_max = 128
node.session.queue_depth = 32
node.session.xmit_thread_priority = -20
node.session.iscsi.InitialR2T = No
node.session.iscsi.ImmediateData = Yes
node.session.iscsi.FirstBurstLength = 262144
node.session.iscsi.MaxBurstLength = 16776192
node.conn[0].iscsi.MaxRecvDataSegmentLength = 262144
node.conn[0].iscsi.MaxXmitDataSegmentLength = 0
discovery.sendtargets.iscsi.MaxRecvDataSegmentLength = 32768
#node.conn[0].iscsi.HeaderDigest = CRC32C,None
#node.conn[0].iscsi.DataDigest = CRC32C,None
node.session.nr_sessions = 1
node.session.iscsi.FastAbort = Yes
"""

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

class ISCSIInitiator(object):
    """
    Library for iSCSI adm related commands
    """

    IscsiTarget = namedtuple('IscsiTarget', ['ip', 'port', 'iqn', 'iface'])

    def __init__(self, system):
        """
        Creates a iscsiadm object for system type
        !! Should not be called directly !
        This should be accessible via systems.filesystem
        property created
        """
        self.system = system
        if system.on_hardware():
            self.max_iscsi_worker = MAX_ISCSI_WORKER
        else:
            self.max_iscsi_worker = 1
        self._iser_is_supported = None  # lazy-loaded


    def _is_iqn_subset_of_discovered_iqns(self, iqns, discovered_iqns):
        return bool(set(iqns) <= set(discovered_iqns))


    def _parse_discovery_result(self, result, port=3260):
        """
        Helper method to parse iscsiadm -m discovery result
        Returns: ip_iqn_dict
        """
        line_pattern = re.compile('(.*)\n')
        ip_iqn_pattern = re.compile('(^\d.*):' + unicode(port) + '.*(iqn.*)')
        # Construct ip_iqn_dict from discovery result which may contain
        # all ifaces/iqns/portals
        ip_iqn_dict = {}
        for x in re.finditer(line_pattern, result):
            line = x.group(0)
            # '172.26.2.253:3260,1
            # iqn.2013-05.com.daterainc:tc:01:sn:fb3d0487dc0e6591'
            match_obj = re.search(ip_iqn_pattern, line)
            if match_obj:
                (discovered_ip, discovered_iqn) = match_obj.groups()
                assert(discovered_ip)
                assert(discovered_iqn)
                ip_iqn_dict.setdefault((discovered_ip, discovered_iqn),
                                       discovered_iqn)
        return ip_iqn_dict


    def logout_all_sessions(self):
        """
        Logs out of all sactive sessions on the host.
        """
        funcs = list()
        kwargs_list = list()
        for session_info in self.list_session_info():
            funcs.append(self.logout)
            kwargs = {"iqn": session_info["target_iqn"],
                      "ip": session_info["target_ip"],
                      "port": session_info["target_port"],
                      "iface_name": None}
            kwargs_list.append(kwargs)
        workers = min(16, len(funcs))
        Parallel(funcs=funcs, kwargs_list=kwargs_list,
                 max_workers=workers).run_threads()


    def logout(self, iqn, ip=None, port=3260, iface_name="default"):
        """
        Logout from iscsi target.

        Parameter:
          iqn (str) - iqn to logout
          ip (str) - ip address to logout off
          port (int) - port number to be used for portal. Default to 3260
          iface_name (str) - interface name to limit operation on
          timeout (int) - Time in secs to attepmt login

        Raises:
          EnvironmentError - If logout fails
        """
        cmd = ["iscsiadm", "-m", "node", "-T", iqn]
        if ip:
            portal = unicode(ip) + ":" + unicode(port)
            cmd.extend(["-p", portal])
        if iface_name is not None:
            cmd.extend(["-I", iface_name])
        cmd.append("--logout")
        cmd_str = ' '.join(cmd)

        # These are error codes and responses found in output of
        # iscsiadm comand. If return codes or output matches any
        # of these which means iscsi interface was deleted
        error_response = ["No matching sessions found",
         "Could not read iface info for"]
        error_code = [3, 7, 21]

        # Perform the iscsiadm logout:
        ret = None
        output = None
        with _ISCSIADM_SEMIS[self.system.name]:
            for ret, output in poll(self.system.run_cmd, args=[cmd_str],
                                    retries=_LOGOUT_RETRY_COUNT,
                                    interval=_ISCSIADM_RETRY_INTERVAL):
                if ret == 0:
                    self.remove_iscsi_loggedin_target_using_iqn(ip, iqn)
                    break
                elif ret in error_code or output in error_response:
                    break  # already logged out
                # Let's draw attention to this annoyance for now:
                if ret == 6:
                    log.warning("System %s temporary iSCSI DB logout error",
                                self.system.name)
            else:
                raise EnvironmentError("%s: cmd:%s failed with exit status:%s,"
                                       "output:%s" % (self.system.name,
                                                      cmd_str, ret, output))


    def destroy_all_ifaces(self):
        """Destroys all ifaces that were created on the host."""
        funcs = list()
        args = list()
        non_deletable_ifaces = ["default", "iser"]
        for iface in self.list_ifaces():
            if iface in non_deletable_ifaces:
                continue
            funcs.append(self.destroy_iface)
            args.append([iface])
        workers = min(16, len(funcs))
        Parallel(funcs=funcs, args_list=args, max_workers=workers).run_threads()


    def remove_target_from_iscsi_loggedin_targets(self, iface_name):
        """
        This method will remove all the targets inside
        ISCSI_LOGGEDIN_TARGETS when they are logged out
        """
        for target in ISCSI_LOGGEDIN_TARGETS[self.system.name][:]:
            if target.iface == iface_name:
                ISCSI_LOGGEDIN_TARGETS[self.system.name].remove(target)


    def force_delete_all(self):
        """
        Force deletes all iSCSI db entries, whether or not they were
        discovered by this object.  Call force_logout_all() before this.

        WARNING: this is potentially a very destructive operation.
        Test cases should never call this method!!!
        """
        self.logout_all_sessions()

        self.destroy_all_ifaces()

        # different linux os have different paths
        # make sure we are deleting the correct one
        if self.system.path_exists("/etc/iscsi/nodes"):
            self.system.run_cmd_check("rm -rf /etc/iscsi/nodes/*")
            if self.system.name in DISCOVERED_TARGETS:
                del DISCOVERED_TARGETS[self.system.name]
            return
        elif self.system.path_exists("/var/lib/iscsi/nodes"):
            self.system.run_cmd_check("rm -rf /var/lib/iscsi/nodes/*")
            self.system.run_cmd_check("rm -rf /var/lib/iscsi/send_targets/*")
            if self.system.name in DISCOVERED_TARGETS:
                del DISCOVERED_TARGETS[self.system.name]
            return
        # in ubuntu these paths only exist if there are active sessions.

    def list_session_info(self):
        """
        Returns a list of dicts with the session info for this format:
            session_info:
                [{
            "target_ip": 172.29.157.71
            "target_port": 3260,
            "target_iqn": iqn.2013-05.com.daterainc:tc:01:sn:95d778f5f581fba2
                },
                ]
        """
        with _ISCSIADM_SEMIS[self.system.name]:
            ret, output = self.system.run_cmd("iscsiadm -m session")
        if "No active sessions." in output:
            return []
        return self._parse_session_result(output)

    def _parse_session_result(self, result):
        """
        Parses the result of iscsi ressions active. i.e. iscsiadm -m session:
        tcp: [149] 172.29.157.71:3260,
            1 iqn.2013-05.com.daterainc:tc:01:sn:95d778f5f581fba2 (non-flash)
        Args:
            result: (str) output of iscsiadm -m session

        Returns:
            data structure with format (list of dicts):
                session_info:
                [{
            "target_ip": 172.29.157.71
            "target_port": 3260,
            "target_iqn": iqn.2013-05.com.daterainc:tc:01:sn:95d778f5f581fba2
                },
                ]
        """
        active_sessions = list()
        rhel_format = "tcp: \[\d+\] (\d+.\d+.\d+.\d+):(\d+),\w+ (iqn.*) .*"

        for session in result.splitlines():
            match = re.match(rhel_format, session)
            if match is not None:
                session_info = {
                    "target_ip": match.groups()[0],
                    "target_port": match.groups()[1],
                    "target_iqn": match.groups()[2]
                }
                active_sessions.append(session_info)
                continue
            msg = ("Failed to match iscsi session info, output:"
                   "\n{} specific line that is failing:\n".format(
                    result, session))
            raise ValueError(msg)
        return active_sessions

    def update_iface(self, iface_name, param, value):
        """
        Updates iface config with param/value.

        Parameter:
          iface_name (str) - iface name to be update
          param (str) - parameter to update
          value (str) - value to update
        """
        cmd = ["iscsiadm", "-m", "iface", "-I", iface_name, "-o", "update",
               "-n", "iface." + param]
        cmd.extend(["-v", unicode(value)])
        cmd_str = ' '.join(cmd)

        with _ISCSIADM_SEMIS[self.system.name]:
            ret, output = self.system.run_cmd(cmd_str)
        if ret != 0:  # Command failed
            # Workaround: On Ubuntu open-iscsi-utils version
            # 2.0.817 iscsiadm completes the update operation
            # successfully but returns exit code 19. This
            # is a bug in iscsiadm in Ubuntu.
            if ret == 19 and 'updated' in output:
                log.warning(
                    'Interface updated but iscsiadm returned '
                    ' exit code: %d', ret)
                log.warning(
                    'Not failing this step as this is iscsiadm bug')
                return
            else:
                log.error("iSCSI iface update failed")
                raise EnvironmentError(
                    "%s: Unable to update iface param:%s to "
                    "value:%s. Return code:%s" % (self.system.name,
                                                  param, value, ret))
        # success


    def list_ifaces(self):
        """
        List ifaces configured on client
        """
        result = {}
        cmd_str = "iscsiadm -m iface"
        with _ISCSIADM_SEMIS[self.system.name]:
            output = self.system.run_cmd_check(cmd_str)
        for line in output.splitlines():
            result[line.split()[0]] = line.split()[1]
        return result


    def _generate_random_string(self, length):
        return ''.join(random.choice(
            string.ascii_uppercase) for i in range(length))


    def get_new_iface(self, iqn=None, allow_eui64=True):
        """
        Create a new iface for the iscsiadm config.
        Attach iqn to new iface. If iqn is not provided, create one.

        Parameter:
           iqn (str) - iqn to attach to iface.  If None, it's generated
           allow_eui64 (bool) - If generating interface name, whether to
                                allow EUI64 format.
        """
        if not iqn:
            if allow_eui64:
                iqn = namegenerator.initiator_name_generator()
            else:
                iqn = namegenerator.iqn_generator()
        iface_name = "iface" + self._generate_random_string(5)
        cmd = ["iscsiadm", "-m", "iface", "-I", iface_name, "-o", "new"]
        cmd_str = ' '.join(cmd)
        with _ISCSIADM_SEMIS[self.system.name]:
            self.system.run_cmd_check(cmd_str)
        self.update_iface(iface_name, "initiatorname", iqn)
        return iface_name


    def get_initiator_name(self, iface_name="default"):
        """
        Returns a string like 'iqn.1993-08.org.debian:01:c722b111bd7e',
        or None if an initiator name could not be determined for this system.
        """
        if not iface_name:
            iface_name = "default"

        if iface_name in ("default", "iser"):
            initiatorname_iscsi = "/etc/iscsi/initiatorname.iscsi"
            if self.system.path_exists(initiatorname_iscsi):
                with self.system.file_open(initiatorname_iscsi, 'r') as initf:
                    filedata = initf.read()
                for line in filedata.splitlines():
                    matchobj = re.match(r'^\s*InitiatorName\s*=\s*(\S+).*$',
                                        line)
                    if matchobj and matchobj.group(1):
                        return matchobj.group(1)

        cmd = ('i=' + pipes.quote(iface_name) +
               ' ; iscsiadm -m iface -I "$i"' +
               " | grep '^iface.initiatorname' | awk '{print $NF}'")
        with _ISCSIADM_SEMIS[self.system.name]:
            iqn = self.system.run_cmd_check(cmd).strip()
        if iqn:
            if iqn.startswith("iscsiadm: Could not read iface"):
                raise ValueError("Invalid iface_name: %r" % iface_name)
            elif iqn.startswith("iscsiadm: Could not read proc_name"):
                raise ValueError("iscsiadm: could not read proc name")
            return iqn

        return None


    def logout_all_targets(self):
        """
        Logout all targets which were logged into by this object
        """
        # for iface scoped targets, lets do batch processing
        # we will do this only for non default ifaces
        if self.system.name in ISCSI_LOGGEDIN_TARGETS:
            ifaces = []
            for target in ISCSI_LOGGEDIN_TARGETS[self.system.name]:
                if target.iface != "default":
                    ifaces.append(target.iface)
            unique_ifaces = list(set(ifaces))
            for iface in unique_ifaces:
                # For unique interfaces, we can assume that only this client
                # instance was used to do the iscsi login, so we can just log
                # them all out with a single command:
                cmd = ["iscsiadm", "-m node", "-I", iface, "-u"]
                cmd_str = ' '.join(cmd)
                with _ISCSIADM_SEMIS[self.system.name]:
                    ret, out = self.system.run_cmd(cmd_str)
                    # no error or exception raised, remove the entries from
                    # list
                    if ret == 0 and self.system.name in ISCSI_LOGGEDIN_TARGETS:
                        self.remove_target_from_iscsi_loggedin_targets(iface)
            # Now do logouts individually for targets using the default
            # interface. Run them in parallel
            funcs_list = []
            args_list = []
            if (self.system.name in ISCSI_LOGGEDIN_TARGETS
                    and ISCSI_LOGGEDIN_TARGETS[self.system.name]):
                for target in ISCSI_LOGGEDIN_TARGETS[self.system.name][:]:
                    funcs_list.append(self.logout)
                    args_list.append((target.iqn, target.ip, 3260,
                                      target.iface))
                del ISCSI_LOGGEDIN_TARGETS[self.system.name]
                pt = Parallel(funcs_list, args_list,
                              max_workers=self.max_iscsi_worker)
                pt.run_threads()


    def attach_all_targets(self, targets, iface_name="default",
                           redirect_mode=None, redirect_ip=None):
        """
        Discover and login into targets
        Parameter:
          targets (list) - list of tuples with following values,
                           "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
          redirect_mode (str) - "Discover" or "Login". Defaults to neither
          redirect_ip (str) - redirect ip address. Must be provided if
                              redirect_mode is not None
        """
        if redirect_mode:
            if not redirect_ip:
                raise ValueError(
                    "redirect ip must be provided when redirect mode is used")
            if redirect_mode.lower() == "discovery":
                self.attach_all_targets_with_discovery_redirect(
                    targets, redirect_ip, iface_name)
            elif redirect_mode.lower() == "login":
                self.attach_all_targets_with_login_redirect(
                    targets, redirect_ip, iface_name)
            else:
                raise ValueError(
                    "Only Discovery OR login redirection mode is supported")
        else:
            self.discover_all_targets(targets, iface_name)
            self.login_all_targets(iface_name=iface_name)

    def attach_all_targets_with_discovery_redirect(self, targets, redirect_ip,
                                                   iface_name="default"):
        """
        Performs iscsi discovery on a list of targets using discovery
        redirection

        Parameter:
          targets (list) - list of tuples with following values,
                           "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
          redirect_ip (str) - redirect ip address. Must be provided if
                              redirect_mode is not None
        """
        # we must replace target ip with
        # redirect ip and everything should function normally
        targets = [(redirect_ip, x[1], x[2]) for x in targets]
        log.debug("{} : {} targets : {}".format(
            self.system.name, iface_name, targets))
        # in discovery redirect we call discovery directly, then login
        self.discover_all_targets(targets, iface_name, iscsi_redirect=True)
        # since our discovered targets should have redirect ip, we can
        # login directly without any changes
        sleep_time = random.choice(xrange(5, 11))
        log.debug(
            "Sleep {} seconds from {} : {} before login_all_targets".format(
                sleep_time, self.system.name, iface_name))
        time.sleep(sleep_time)
        self.login_all_targets(iface_name=iface_name)

    def attach_all_targets_with_login_redirect(self, targets, redirect_ip,
                                               iface_name="default"):
        """
        Performs iscsi discovery on a list of targets using login
        redirection

        Parameter:
          targets (list) - list of tuples with following values,
                           "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
          redirect_ip (str) - redirect ip address. Must be provided if
                              redirect_mode is not None
        """
        # we must replace target ip with
        # redirect ip and everything should function normally
        targets = [(redirect_ip, x[1], x[2]) for x in targets]
        # for all login redirect, we do not use the discovery service but
        # manually add the info to the DB
        for target in targets:
            # manually discover
            self.manually_add_target_to_db(ip=target[0],
                                           port=target[1],
                                           iqn=target[2],
                                           iface_name=iface_name)

        # since our discovered targets should have redirect ip, we can
        # login directly without any changes
        self.login_all_targets(iface_name=iface_name)


    def manually_add_target_to_db(self, ip, port, iqn, iface_name="default"):
        """
        Manually adds a target to the iscisadm DB without discovery. This
        should be used only if you are using client.iscsi.login() with
        redirect ip provided as ip in the method. Preferred way is to use
        attach_all_targets()/detach_all_targets() which automatically takes
        care of discovery / login / cleanup based on redirection mode if
        desired.

        Parameter:
          targets (list) - list of tuples with following values,
                           "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
          redirect_ip (str) - redirect ip address. Must be provided if
                              redirect_mode is not None
        """
        log.debug("iSCSI manuall add ip=%s iqn=%s", ip, iqn)
        # create portal address
        portal = unicode(ip) + ":" + unicode(port)
        # construct the cmd
        cmd = ["iscsiadm", "-m", "node"]
        if iface_name:
            cmd.extend(["-I", iface_name])
        cmd.extend(["-T", iqn, "-p", portal, "-o", "new"])

        cmd_str = ' '.join(cmd)
        log.debug("command = " + repr(cmd_str))
        self.system.run_cmd_check(cmd_str)
        # confirm discovery is not needed
        if self._is_target_discovery_needed((ip, port, iqn), iface_name):
            raise EnvironmentError(
                "Manually added target not in discovered list")

    def remove_iscsi_loggedin_target_using_iqn(self, ip, iqn):
        """
        Remove the target from ISCSI_LOGGEDIN_TARGETS

        Parameter:
          iqn (str) - iqn to logout
          ip (str) - ip address to logout off
        """
        if (self.system.name in ISCSI_LOGGEDIN_TARGETS and
                not ISCSI_LOGGEDIN_TARGETS[self.system.name]):
            for target in ISCSI_LOGGEDIN_TARGETS[self.system.name][:]:
                if target.iqn == iqn:
                    if ip:
                        if target.ip == ip:
                            ISCSI_LOGGEDIN_TARGETS[self.system.name].remove(
                                target)
                            break
                        else:
                            log.debug(
                                "logout: iqn matched but ip did not, retrying")
                    else:
                        ISCSI_LOGGEDIN_TARGETS[self.system.name].remove(
                            target)

    def _is_target_discovery_needed(self, target, iface_name="default"):
        """
        Return True/False if discovery is needed for a target. If discovery
        is not needed but a target is missing from the DISCOVERED_TARGETS
        it will be appended to the above list

        Parameter:
          target (tuple) - tuple with following values, "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
        Raises:
          EnvironmentError - If iqn is not discovered
        """
        # get already discovered targets
        discovered_targets = self.list_all_discovered_targets()

        tgt_ip = target[0]
        tgt_port = target[1]
        tgt_iqn = target[2]

        discovery_needed = False
        # see if target in discovered_targets
        if tgt_iqn in discovered_targets:
            # validate all other values like portal and iface
            ip_portal = unicode(tgt_ip) + ":" + unicode(tgt_port)
            discovered_target = discovered_targets[tgt_iqn]
            if ip_portal in discovered_target['portals']:
                idx = discovered_target['portals'].index(ip_portal)
                if iface_name == discovered_target['ifaces'][idx]:
                    # we have a complete match, add to discovered list
                    target = self.IscsiTarget(tgt_ip, tgt_port, tgt_iqn,
                                              iface_name)
                    if self.system.name not in DISCOVERED_TARGETS:
                        DISCOVERED_TARGETS[self.system.name] = []
                        DISCOVERED_TARGETS[self.system.name].append(target)
                    else:
                        if target not in DISCOVERED_TARGETS[self.system.name]:
                            DISCOVERED_TARGETS[self.system.name].append(
                                target)
                else:
                    discovery_needed = True
            else:
                discovery_needed = True
        else:
            discovery_needed = True

        return discovery_needed

    def list_all_discovered_targets(self):
        with _ISCSIADM_SEMIS[self.system.name]:
            ret, out = self.system.run_cmd("iscsiadm -m node -P 1")
        if ret:
            return {}
        data = out.split("Target: ")
        # remove null values
        data = [x for x in data if x]
        result = {}
        for line in data:
            block_result = {}
            block_result['portals'] = []
            block_result['ifaces'] = []
            blk_lines = line.splitlines()
            target = blk_lines[0]
            # parse portals. Since lists maintain positions we can do them all
            # at once
            for i in blk_lines[1:]:
                if "Portal" in i:
                    val = i.split("Portal: ")[-1]
                    portal = val.split(",")[0]
                    block_result['portals'].append(portal)
                elif "Iface" in i:
                    iface_name = i.split("Iface Name: ")[-1]
                    block_result['ifaces'].append(iface_name)
                else:
                    raise ValueError("Unexpected value: %s" % i)
            result[target] = block_result
        return result


    def discover(self, ip, iqn=None, port=3260,
                 iface_name="default",
                 timeout=_DISCOVERY_TIMEOUT):
        """
        Performs iscsi discovery on a target address.
        Parameter:
          ip (str) - ip address to perform discovery on
          iqn (str) - iqn to discover or list of iqns to match discovery result
          port (int) - port number to be used for portal. Default to 3260
          iface_name (str) - interface name to limit discovery on
          timeout (int) - Time in secs to attempt discovery

        Raises:
          EnvironmentError - If iqn is not discovered

        Note:
          if a list of iqn is provided,
          iface_name will not be used during discovery
          then whole discovery result will be parsed and the list of iqns
          matching the result will be added to DISCOVERED_TARGETS
          Eg. self.discover(ip=ip, iqn=iqn_list,
                            port=port, iface_name=iface_name)
        """
        log.debug("iSCSI discovery ip=%s iqn=%s", ip, iqn)
        # create portal address
        portal = unicode(ip) + ":" + unicode(port)
        iqns = None
        # construct the cmd
        cmd = ["iscsiadm", "-m", "discovery"]

        cmd.extend(["-t", "sendtargets", "-p", portal])
        if iqn:
            if isinstance(iqn, list):
                iqns = iqn
                # no iface_name added
            else:
                if iface_name:
                    cmd.extend(["-I", iface_name])
                cmd.extend(["|", "grep", iqn])
                iqns = [iqn]
        else:
            msg = "iqn is required"
            raise EnvironmentError(msg)

        cmd_str = ' '.join(cmd)
        log.debug("command = " + repr(cmd_str))

        start_time = time.time()
        gen = poll(function=self.system.run_cmd, args=[cmd_str],
                   retries=_DISCOVERY_RETRY_COUNT,
                   interval=_ISCSIADM_RETRY_INTERVAL)
        for status, result in gen:
            # verify if iqn is discovered
            log.debug("Discovery Returned status = " +
                      repr(status))
            if status == 0 and result:
                ip_iqn_dict = self._parse_discovery_result(result)
                # The size of ip_iqn_dict should be the same or double the iqns
                num_iqns = len(iqns)
                num_discovered_iqns = len(ip_iqn_dict.keys())
                log.debug("num_iqns : {} , num_discovered_iqns : {}\n"
                          .format(num_iqns, num_discovered_iqns))
                # log.info("iqns : {} \n ip_iqn_dict.values :{}".format(
                #    iqns, pformat(ip_iqn_dict.values())))
                if num_discovered_iqns == 0:
                    log.debug("Number of discovered iqns :0, retry discovery")
                    continue
                # iqn list should be a subset of the discovered ip_iqns
                # otherwise retry
                if self._is_iqn_subset_of_discovered_iqns(iqns, ip_iqn_dict.values()):
                    # all iqns are discovered
                    # add all ip_iqn_dict with matching iqns to
                    # discovered_targets
                    for ip_iqn, iqn in ip_iqn_dict.viewitems():
                        if iqn in set(iqns):
                            (ip, iqn) = ip_iqn
                            target = self.IscsiTarget(
                                ip, port, iqn, iface_name)
                            if self.system.name not in DISCOVERED_TARGETS:
                                DISCOVERED_TARGETS[self.system.name] = []
                                log.debug("add target :\n{}".format(target))
                                DISCOVERED_TARGETS[self.system.name].append(
                                    target)
                            else:
                                if target not in DISCOVERED_TARGETS[self.system.name]:
                                    DISCOVERED_TARGETS[self.system.name].append(
                                        target)
                    # TODO: Add some assert ?
                    return

            if time.time() - start_time > timeout:
                break
            msg = "{} : {} iSCSI discovery ip:{} iqn:{} failed, will try again"\
                  .format(self.system.name, iface_name, ip, iqn)
            log.debug(msg)
        else:
            msg = ("%s: IQN:%s not found in discovery result:\n" +
                   "%s\nwith cmd: %s") % (self.system.name, iqn, result, cmd_str)
            raise EnvironmentError(msg)


    def discover_all_targets(self, targets, iface_name="default",
                             iscsi_redirect=None):
        """
        Performs iscsi discovery on a list of target addresses.

        Parameter:
          targets (list) - list of tuples with following values,
                           "ip, port, iqn"
          iface_name (str) - interface name to limit discovery on
        Raises:
          EnvironmentError - If iqn is not discovered
        """
        if iscsi_redirect is True:
            # Save all the iqns from targets to tgt_iqns
            # Pass tgt_iqns to discover()
            tgt_iqns = []
            for _, target in enumerate(targets):
                (tgt_ip, tgt_port, tgt_iqn) = target
                tgt_iqns.append(tgt_iqn)

            self.discover(ip=tgt_ip, iqn=tgt_iqns,
                          port=tgt_port, iface_name=iface_name)

        else:
            funcs = list()
            kwargs = list()
            for _, target in enumerate(targets):
                (tgt_ip, tgt_port, tgt_iqn) = target
                funcs.append(self.discover)
                kwargs.append({"ip": tgt_ip,
                               "iqn": tgt_iqn,
                               "port": tgt_port,
                               "iface_name": iface_name})
            Parallel(funcs=funcs, kwargs_list=kwargs).run_threads()


    def login(self, iqn, ip=None, port=3260, iface_name="default", timeout=20):
        """
        Login to iscsi target.  Target must be discovered first.

        Parameter:
          iqn (str) - iqn to discover
          ip (str) - ip address to login on
          port (int) - port number to be used for portal. Default to 3260
          iface_name (str) - interface name to limit discovery on
          timeout (int) - Time in secs to attepmt login

        Raises:
          EnvironmentError - If login fails
        """
        with _ISCSIADM_SEMIS[self.system.name]:
            cmd = ["iscsiadm -m node -T", iqn]
            if ip:
                portal = unicode(ip) + ":" + unicode(port)
                cmd.extend(["-p", portal])
            if iface_name:
                cmd.extend(["-I", iface_name])
            cmd.append("--login")
            cmd_str = ' '.join(cmd)
            gen = poll(function=self.system.run_cmd, args=[cmd_str],
                       retries=timeout, interval=1)
            for status, result in gen:
                if status == 0:
                    target = self.IscsiTarget(ip, port, iqn, iface_name)
                    if self.system.name not in ISCSI_LOGGEDIN_TARGETS:
                        ISCSI_LOGGEDIN_TARGETS[self.system.name] = []
                        ISCSI_LOGGEDIN_TARGETS[self.system.name].append(target)
                    else:
                        if target not in ISCSI_LOGGEDIN_TARGETS[self.system.name]:
                            ISCSI_LOGGEDIN_TARGETS[self.system.name].append(target)
                    return
                if status in [3, 8, 21]:
                    # If 8 and 21 these codes pop which means
                    # the target was already logged out
                    # If we see error code 3 which means
                    # iface was deleted
                    return
                log.debug("iSCSI login failed" + cmd_str + "\n" + result)
            msg = ("%s: iSCSI login to IQN:%s FAILED with" +
                   " status:%s, output=%s using cmd:%s") % (self.system.name,
                                                            iqn, status,
                                                            result, cmd_str)
            raise EnvironmentError(msg)


    def login_all_targets(self, udev_timeout=_UDEV_TIMEOUT,
                          iface_name="default"):
        """
        Do an iSCSI login to all discovered targets
        Args:
            udev_timeout: (int) the default udev timeout is 120 seconds,
                this can become a problem with high volumes of
        """
        targets = [target for target in DISCOVERED_TARGETS[self.system.name]
                   if target.iface == iface_name]
        log.debug("{}, iface: {}, targets: {}, DISCOVERED_TARGETS: {}"
                  .format(self.system.name, iface_name,
                          len(targets), len(DISCOVERED_TARGETS[self.system.name])))
        if len(targets) == 0:
            msg = "Num of discovered targets with iface_name {} is 0".format(
                iface_name)
            log.warning(msg)
        funcs_list = []
        args_list = []
        for target in DISCOVERED_TARGETS[self.system.name]:
            funcs_list.append(self.login)
            args_list.append((target.iqn, target.ip, 3260, target.iface))
        pt = Parallel(funcs=funcs_list, args_list=args_list,
                      max_workers=self.max_iscsi_worker)
        pt.run_threads()

        cmd = "sleep 1 ; udevadm trigger ; sleep 1 ;" \
              " udevadm settle --timeout=%s" % udev_timeout
        with _UDEV_SEMIS[self.system.name]:
            self.system.run_cmd_check(cmd)


    def delete_all_targets(self):
        """
        Delete iSCSI record of targets discovered by this object
        """
        # for iface scoped targets, lets do batch processing
        # we will do this only for non default ifaces
        if self.system.name in DISCOVERED_TARGETS:
            ifaces = []
            for target in DISCOVERED_TARGETS[self.system.name]:
                if target.iface != "default":
                    ifaces.append(target.iface)
            unique_ifaces = list(set(ifaces))
            for iface in unique_ifaces:
                if "default" not in iface:
                    # XXX - pretty sure multiple ifaces doesn't work in this
                    # library but adding support here so we can fix the other
                    # issues later on.
                    self.destroy_iface(iface)
            # Performing this operation in parallel leads to db errors
            # leaving it single threaded for now as the operation is
            # fairly quick
            if (self.system.name in DISCOVERED_TARGETS and
                    DISCOVERED_TARGETS[self.system.name]):
                for target in DISCOVERED_TARGETS[self.system.name][:]:
                    self.delete(ip=target.ip, iqn=target.iqn)


    def delete(self, iqn, ip=None):
        """
        Delete iSCSI record of the target

        Parameter:
          iqn (str) - iqn to logout
          ip  (str) - portal ip address

        Raises:
          EnvironmentError - If logout fails
        """
        cmd = ["iscsiadm", "-m", "node", "-T", iqn]
        if ip:
            cmd.extend(["-p", ip])
        cmd.extend(["--op", "delete"])
        cmd_str = ' '.join(cmd)

        # Perform the iscsiadm delete:
        ret = None
        output = None
        with _ISCSIADM_SEMIS[self.system.name]:
            for ret, output in poll(self.system.run_cmd, args=[cmd_str],
                                    retries=_DELETE_RETRY_COUNT,
                                    interval=_ISCSIADM_RETRY_INTERVAL):
                if ret == 0:
                    break  # success
                elif ret == 21 or "No records found" in output:
                    break  # already deleted
                # Let's draw attention to this annoyance for now:
                if ret == 6:
                    log.warning("System %s temporary iSCSI DB delete error",
                                self.system.name)
            else:
                raise EnvironmentError("%s: cmd:%s failed with exit status:%s,"
                                       "output:%s" % (self.system.name, cmd_str,
                                                      ret, output))

        # Book-keeping: Remove the target from DISCOVERED_TARGETS:
        if (self.system.name in DISCOVERED_TARGETS and
                DISCOVERED_TARGETS[self.system.name]):
            for target in DISCOVERED_TARGETS[self.system.name][:]:
                if target.iqn == iqn:
                    DISCOVERED_TARGETS[self.system.name].remove(target)


    def destroy_iface(self, iface_name):
        """
        Delete iface interface

        Parameter:
          iface_name (str) - iface name to be update
        """
        # logout of all sessions for the iface
        self.force_logout_all(iface_name=iface_name)
        cmd = "iscsiadm -m iface -I " + iface_name + " -o delete"
        # There are cases when this command can fail, needs a retry
        retries = 10
        while retries > 0:
            with _ISCSIADM_SEMIS[self.system.name]:
                ret, out = self.system.run_cmd(cmd)
                if ret == 0:
                    break
                elif ret == 6:
                    retries -= 1
                    time.sleep(5)
                elif ret != 0:
                    # If we see following string in output which means
                    # that iface was deleted. Hence, we will continue with
                    # the test
                    if ("Could not delete iface" or "Could not read iface info"
                            in out):
                        return
                    raise EnvironmentError("Command failed ($?={} on"
                                        "{}:\n {} \n {}".format(ret,
                                            self.system.name, cmd, out))
        # remove the targets from discovered list
        if (self.system.name in DISCOVERED_TARGETS and
                DISCOVERED_TARGETS[self.system.name]):
            for target in DISCOVERED_TARGETS[self.system.name][:]:
                if target.iface == iface_name:
                    DISCOVERED_TARGETS[self.system.name].remove(
                        target)


    def force_logout_all(self, iface_name="default"):
        """
        WARNING: this is potentially a very destructive operation.
        Test cases should never call this method!!!

        Logout from all active iscsi sessions, whether or not they were
        logged into by this object.

        Parameter:
          iface_name (str) - interface name to limit operation on
        """
        cmd = ["iscsiadm", "-m", "node"]
        if iface_name:
            cmd.extend(["-I", iface_name])
        cmd.extend(["-u", "all"])
        cmd_str = ' '.join(cmd)
        with _ISCSIADM_SEMIS[self.system.name]:
            ret, output = self.system.run_cmd(cmd_str)
        if ret != 0:
            # Added few string to be checked if there is error
            # These strings means either session deleted or iface
            # was not found since it was deleted by thread before
            # Hence we can continue with the test
            if ("No matching sessions found" or "Could not read iface info for"
                    in output):
                return
            raise EnvironmentError("%s: cmd:%s failed with exit status:%s,"
                                   "output:%s" % (self.system.name, cmd_str,
                                    ret, output))
        if iface_name:
            if(self.system.name in ISCSI_LOGGEDIN_TARGETS and
                    ISCSI_LOGGEDIN_TARGETS[self.system.name]):
                self.remove_target_from_iscsi_loggedin_targets(
                    iface_name)
        else:
            del ISCSI_LOGGEDIN_TARGETS[self.system.name]
