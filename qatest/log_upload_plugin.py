# -*- coding: utf-8 -*-
"""
Provides the LogUploadPlugin class, for moving logs to executor when tests don't pass
"""
import functools
import logging
import nose
import subprocess
import os
import json

from qalib.qabase.threading import Parallel
import qalib.client
from qalib.qabase.constants import POSIXCLIENT_QA_LOGDIR

__copyright__ = 'Copyright 2020, Datera, Inc.'

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class LogUploadPlugin(nose.plugins.Plugin):
    """
    A nose plugin to upload log files
    on test case failures.
    """
    enabled = True
    name = 'loguploadplugin'
    # score must be greater than output plugin scores
    score = 1185

    def __init__(self, logdir, equipment):
        """
        Parameters:
          logdir (str)
          equipment (qacorelibs.equipmentprovider.EquipmentProvider)
        """
        super(LogUploadPlugin, self).__init__()
        self.logger = logging.getLogger(__name__)
        self.equipment = equipment
        self.log_dest_dir = logdir
        self.upload_required = False
        self.client_uploader = ClientUploader.from_equipment(equipment)
        self.cluster_uploader = ClusterUploader.from_equipment(equipment)

    def configure(self, options, conf):
        pass

    def options(self, parser, env):
        pass

    def beforeTest(self, test):
        self.upload_required = False
        self.wrap_testcase_setup(test.test)
        self.wrap_testcase_teardown(test.test)

    def addError(self, _test, _err):
        """ The test case hit an exception """
        self.upload_required = True

    def addFailure(self, _test, _err):
        self.upload_required = True

    def wrap_testcase_setup(self, testcase):
        """
        Wraps the testcase setUp() so that if it fails, we then upload logs
        (Note that if setUp() fails, tearDown() never runs.)
        """
        orig_method = getattr(testcase, 'setUp', None)

        def setUp():
            """ Test case setUp """
            # self.diaguploader.prepare_equipment()
            try:
                orig_method_success = False
                if orig_method is not None:
                    orig_method()
                orig_method_success = True
            # except unittest.SkipTest:
            #     orig_method_success = True
            #     raise
            finally:
                if not orig_method_success:
                    self.upload_required = True
                if self.upload_required:
                    self.upload_logs(testcase.id)

        if orig_method:
            functools.update_wrapper(setUp, orig_method)
        testcase.setUp = setUp

    def wrap_testcase_teardown(self, testcase):
        """
        Wraps the testcase tearDown() so that after it's called, we
        then upload logs if needed.
        """
        orig_method = getattr(testcase, 'tearDown', None)

        def tearDown():
            """ Test case tearDown """
            try:
                orig_method_success = False
                if orig_method is not None:
                    orig_method()
                orig_method_success = True
            # except unittest.SkipTest:
            #     orig_method_success = True
            #     raise
            finally:
                if not orig_method_success:
                    self.upload_required = True
                if self.upload_required:
                    self.upload_logs(testcase)

        if orig_method is not None:
            functools.update_wrapper(tearDown, orig_method)
        testcase.tearDown = tearDown

    def upload_logs(self, testcase):
        # TODO[jsp]: if setUp fails testcase.logdir may not exist
        # so need to find somewhere else to upload these.. self.log_dest_dir
        # is probably not the best place to do this
        if not hasattr(testcase, 'logdir'):
            upload_dest = self.log_dest_dir
        else:
            upload_dest = testcase.logdir
        self.client_uploader.upload(upload_dest)
        self.cluster_uploader.upload(upload_dest)


class ClientUploader(object):
    """ Uploads logs from client systems """

    def __init__(self, client_list):
        self._client_list = client_list

    @classmethod
    def from_equipment(cls, equipment):
        clients = qalib.client.list_from_equipment(equipment,
                                                   required=False)
        return cls(clients)

    def upload(self, testcase):
        if not self._client_list:
            logger.debug("No client systems found, no nothing to upload")
            return
        fns = [self._process_client for _ in self._client_list]
        args_list = [[client, testcase] for client in self._client_list]
        Parallel(fns,
                 args_list=args_list,
                 max_workers=len(self._client_list)).run_threads()

    def _process_client(self, client, logdir):
        basedir = client.mkdtemp()
        subdir = 'client-' + _hostname_simple(client.name)
        workdir = basedir + '/' + subdir
        cmd = 'umask 022 ; mkdir -p ' + workdir
        client.run_cmd_check(cmd)

        logger.info("Gathering logs from client-{}".format(client.name))

        # Gather output of various commands
        cmd = 'umask 022 ;'
        # TODO[jsp]: there are more commands to run defined in upload_corefiles.py
        for infocmd in ("lsscsi", "iscsiadm -m node", "multipath -ll",
                        "iscsiadm -m session", "iscsiadm -m iface",
                        "iscsiadm -m session -P2",
                        "iscsiadm -m node -P1", "iscsiadm -m iface -P1",
                        "ip addr show", "ip link show", "ip route show",
                        "ip neighbor show", "arp -a",
                        "netstat -an",
                        "mount", "df -h",
                        "ls -l /dev/disk/by-uuid",
                        "dmesg -T", "lsscsi --transport", "ps -efww"):
            outfilename = infocmd.replace(' ', '_').replace('/', '_') + \
                ".txt"
            outfilepath = workdir + '/' + outfilename
            # we run the command in the background in case it hangs
            cmd += " ( " + infocmd + " ) </dev/null > " + \
                outfilepath + " 2>&1 &"
        client.run_cmd(cmd)

        logger.info("Uploading logs from client-{}".format(client.name))
        upload_cmd = ["sshpass", "-p", client._system_conn._creds.get_password(),
                      "rsync", "-a", "-r", "--copy-links", "--sparse",
                      "-e", "ssh -o StrictHostKeyChecking=no",
                      "{}@{}:{}".format(client._system_conn._creds.get_username(),
                                        client.ip,
                                        workdir),
                      logdir]

        logger.debug(" ".join(upload_cmd))
        subprocess.call(upload_cmd)

        # Gather various files and directories
        filelist = ('/var/log/messages', '/var/log/syslog',
                    '/etc/os-release',
                    '/etc/multipath.conf')
        cmd = 'umask 022 ; for f in ' + ' '.join(filelist) + ' ; do'
        cmd += ' [ -f "$f" ] && ln -sf -- "$f" ' + workdir + '/ ; done'

        dirlist = ('/etc/iscsi', '/etc/udev', '/var/tmp/datera_witness_log')
        cmd += ' ; for d in ' + ' '.join(dirlist) + ' ; do'
        cmd += ' [ -d "$d" ] && ln -s -- "$d" ' + workdir + '/ ; done'

        cmd += ('; [ -d "%s" ] && ln -s -- "%s" "%s"/' %
                (POSIXCLIENT_QA_LOGDIR, POSIXCLIENT_QA_LOGDIR, workdir))

        # TODO: this doesn't belong in here like this, since it depends
        #       on inner workings of the qalib.load package:
        cmd += ' ; for f in /tmp/fio.* /var/tmp/fio.* /var/tmp/IO.* ; do'
        cmd += ' [ -f "$f" ] && ln -sf "$f" ' + workdir + '/ ; done'

        cmd += ' ; cd ' + basedir
        logger.debug(cmd)
        client.run_cmd_check(cmd)

        # # Upload it all:
        logger.debug(" ".join(upload_cmd))
        subprocess.call(upload_cmd)
        cleanup = 'cd /; rm -rf -- ' + basedir
        client.run_cmd_check(cleanup)


class ClusterUploader(object):
    """ Upload logs from storage nodes """

    def __init__(self, cluster, whiteboxes, logger=None):
        # TODO[jsp]: necessary?
        if logger is None:
            logger = logging.getLogger(__name__)
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())
        self._logger = logger
        self._cluster = cluster
        self._whitebox_list = whiteboxes

    @classmethod
    def from_equipment(cls, equipment):
        """
        Parameters:
          equipment (qalib.equipment.EquipmentProvider)
        """
        cluster = equipment.get_cluster(required=True)
        whitebox_list = qalib.whitebox.list_from_equipment(equipment)
        return cls(cluster, whitebox_list)

    def upload(self, logdir):
        # TODO kill this if it takes too long! exec_command timeout?
        self._logger.info("Beginning cluster log upload. This may take some time.")
        fns = [self._process_node for _ in self._whitebox_list]
        args_list = [[node, logdir] for node in self._whitebox_list]
        Parallel(fns,
                 args_list=args_list,
                 max_workers=len(self._whitebox_list)).run_threads()

    def _process_node(self, whitebox, logdir):
        self._logger.info("Collecting logs on node-{}".format(whitebox.name))
        out_raw = whitebox.cli.exec_command("logcollect --json --local",
                                            timeout=7200)
        out = json.loads(out_raw)
        if "error" in out and out["error"] is not None:
            self._logger.error("Encountered an error during log upload from node-{}: {}".\
                               format(whitebox.name, out["error"]))
        else:
            subdir = 'node-' + _hostname_simple(whitebox.name)
            destdir = os.path.join(logdir, subdir)
            os.mkdir(destdir)
            self._logger.info("Beginning log upload from node-{}".format(
                whitebox.name))
            try:
                logs_on_node = out["result"]["url"]
            except KeyError:
                self._logger.debug(out_raw)  # TODO[jsp]: consider pretty-printing this
                self._logger.critical("Couldn't find log upload path from node-{}".format(
                    whitebox.name))
                return
            sync = [
                "wget", logs_on_node,
                "-P", destdir
            ]
            try:
                self._logger.debug(" ".join(sync))
                subprocess.call(sync)
                self._logger.info("Finished uploading logs from {}".format(
                    whitebox.name))
            except subprocess.CalledProcessError as err:
                self._logger.error("Failed to upload logs from {}: {}".format(
                    whitebox.name, str(err)))

def _hostname_simple(hostname):
    """ Strips off domain name, ".(none)", etc """
    if hostname[0].isdigit():
        return hostname  # IP address
    return hostname.split('.')[0]  # strip domain name
