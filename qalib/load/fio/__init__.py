# -*- coding: utf-8 -*-
"""
Provides a FIO object for running IO.
"""

from __future__ import (unicode_literals, print_function, division)

from pprint import pformat
from random import randint
import datetime
import logging
import threading
import time

from qalib.qabase.formatters import human_readable_size
from qalib.load.fio.workload import override_default_workload_params
from qalib.load.fio.workload import generate_fio_workload
from qalib.load.fio.workload import generate_fio_workload_with_mountpoint
from qalib.qabase.polling import poll
from . import logfile_parser
from .install_on_client import install_fio
from ..baseio import IoBase

__copyright__ = "Copyright 2020, Datera, Inc."

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

_DEFAULT_VERSION = "2.13-noshmem"


class FIO(IoBase):
    """
    Runs fio on a remote Linux system
    if the fio job fails for any reason:
        1.) go to the client that the fio session was run on.
        2.) get the fio ID from the failure output:
            Exception example:
                File "/src/qa/qalib/load/fio/__init__.py", line 290, in _get_st
                ats
                    raise EnvironmentError(msg)
                EnvironmentError: fio id=ec9889c4dcbd11e59d9e00269eb5da06 on cl
                ient rts65 to.....

        3.) look at /var/tmp/ for the two files for each run:
            ls /var/tmp/ |grep ec9889c4dcbd11e59d9e00269eb5da06
                IO.ec9889c4dcbd11e59d9e00269eb5da06.out
                fio.ec9889c4dcbd11e59d9e00269eb5da06.cfg
        4.) verify your configuration with the fio.<fio-id>.cfg file
        5.) verify the error wth the IO.<fio-id>.out file
    """

    def __init__(self,
                 iospec,
                 version=None,
                 interval=10,
                 cleanup=True,
                 startup_timeout=90,
                 force_jobname=None,
                 force_one_job=False,
                 **workload_params):
        """
        Do not instantiate directly; use package from_*() functions
          iospec
          version (str) - FIO version string
          interval (int)
          cleanup (bool)
          startup_timeout (int)
          force_jobname (str)
          force_one_job (bool) this will collapse all block devices into one
           job.  Helpful when using many block devices.
        Additional keyword arguments are the FIO workload
        """
        super(FIO, self).__init__(iospec)
        self._lock = threading.Lock()

        self._iospec = iospec
        self._client = self._iospec.get_client()
        self._remote_workdir = "/var/tmp/fio." + self._id + ".dir"
        self._stop_trigger = self._remote_workdir + "/" + "stop_trigger"
        self.startup_timeout = startup_timeout

        # Set defaults as needed
        if version is None:
            version = _DEFAULT_VERSION
        self._fio_version = version

        self.remote_workloadfile = None

        self._force_jobname = force_jobname
        self._force_one_job = force_one_job
        self.interval = int(interval)  # TODO: make this private
        self.cleanup = cleanup
        self._running = False
        self._should_run = False
        self._thread = None
        # TODO: make this private:
        self.parser = None  # logfile parser

        if self._client.os != "Linux":
            raise ValueError("fio requires a Linux client")

        self._workload_params = workload_params

        # Massage the input workload as needed
        #
        # Reasonable defaults:
        if 'random_generator' not in self._workload_params:
            self._workload_params['random_generator'] = 'tausworthe64'
        if 'ioengine' not in self._workload_params:
            self._workload_params['ioengine'] = 'libaio'
        if 'direct' not in self._workload_params:
            self._workload_params['direct'] = 1
        # If they specify a runtime, default to time_based=1
        if int(self._workload_params.get('runtime', 0)) != 0:
            if 'time_based' not in self._workload_params:
                self._workload_params['time_based'] = 1
        # For write workloads, default to do_verify=0, verify=crc32c-intel:
        if self._workload_params.get('rw', '') in ['write', 'randwrite']:
            if 'do_verify' not in self._workload_params:
                self._workload_params['do_verify'] = 0
                self._workload_params['verify'] = 'crc32c-intel'
        # TODO: we used to have this code, but do we actually want it?:
        #    if "verify_pattern" not in self._workload_params:
        #        verify_pattern = "0x%08x" % randint(0, 2 ** 32 - 1)
        #        self._workload_params["verify_pattern"] = verify_pattern
        # If do_verify is specified, make sure the verify technique is, too:
        if 'do_verify' in self._workload_params:
            if 'verify' not in self._workload_params:
                self._workload_params['verify'] = 'crc32c-intel'
        if "dedupe_percentage" in self._workload_params:
            if "randseed" not in self._workload_params:
                # when dedupe is specified, you need to specify randseed.
                self._workload_params["randseed"] = randint(0, 2 ** 32 - 1)

    def __str__(self):
        output_str = ""
        output_str += "{:>7}: {}\n".format('fio id', str(self._id))
        output_str += "{:>7}: {}\n".format('client', self._client.name)
        output_str += "{:>7}: {}\n".format('params',
                                           str(self._workload_params))
        output_str += "{:>7}: {}\n".format('iospec', str(self._iospec))
        return output_str

    def _logging_prefix(self):
        """
        Human-readable description for log messages
        """
        return self.__str__()

    @staticmethod
    def _override_default_workload_params(default_params, user_params):
        """
        Used to over-ride default parameters with user-specified parameters
        """
        return override_default_workload_params(default_params, user_params)

    def _install_fio_create_workload_build_cmd(self, blockdev_list=None,
         mount_point_list=None):
        """
        Returns the fio command-line
        """
        workload_cfg = None
        # workload for blockdevice
        if blockdev_list:
            workload_cfg = generate_fio_workload(
                blockdev_list=blockdev_list,
                workload_params=self._workload_params,
                force_jobname=self._force_jobname,
                force_one_job=self._force_one_job)
        # workload for filesystem
        elif mount_point_list:
            workload_cfg = generate_fio_workload_with_mountpoint(
                mount_point_list=mount_point_list,
                workload_params=self._workload_params,
                force_jobname=self._force_jobname,
                force_one_job=self._force_one_job)

        # Install FIO on the client:
        remote_path = install_fio(self._client, self._fio_version)

        # create remote fio workload file
        remote_workloadfile = "/var/tmp/fio." + self._id + ".cfg"
        with self._client.file_open(remote_workloadfile, 'w') as wlf:
            wlf.write(workload_cfg)
        log.debug(remote_workloadfile + "\n" + workload_cfg)
        self.remote_workloadfile = remote_workloadfile

        # Command to launch fio with the given workload, with its output
        # redirected
        cmd = "mkdir -p " + self._remote_workdir + " ; " + \
              "cd " + self._remote_workdir + " ; " + \
              "ulimit -n 8192 ; " + \
              remote_path + " " + remote_workloadfile + " " + \
              "--output-format=json" + " " + \
              ("--status-interval=%s" % self.interval) + " " + \
              "--trigger-file=" + self._stop_trigger + " " \
              " > " + self._output_file + " 2>&1"
        return cmd

    def stop(self):
        """ Stops an FIO instance previously started with start() """
        try:
            self._stop()
        finally:
            super(FIO, self).stop()

    def _stop(self):
        """ Stop FIO, raise IOError if there were errors """
        if not self._running:
            return
        self._should_run = False
        # stopping the async thread
        if self._thread is not None:
            self._thread.join()
        # Trigger fio to exit:
        self._client.run_cmd("touch " + self._stop_trigger)

        # Wait for fio to exit
        wait_ioerror = None
        try:
            self.wait()
        except IOError as ex:
            # Don't raise the exception yet; we should check the fio log
            # for useful error messages first
            wait_ioerror = ex

        # Check for any error messages in the logs:
        self.check_for_errors()

        # If wait() failed above, raise that exception now:
        if wait_ioerror is not None:
            msg = self._logging_prefix() + ": " + str(wait_ioerror)
            raise IOError(msg)

        # If there were no errors, make sure SOME IO happened
        self._verify_io_happened()

        # no errors and IO happened, cleaning up.
        if self.cleanup:
            self._cleanup_output_file()
            self._cleanup_remote_workloadfile()
            self._cleanup_remote_workdir()
        self.parser.close()
        self._running = False

    def start(self):
        """
        Starts I/O.  May be used as a context manager.
        """
        self._setup_output_file()
        self._should_run = True
        # Determine which block devices to do I/O to:
        blockdev_list = self._iospec.get_blockdev_list()
        num_disks = 0
        mount_list = None
        # if no blockdevices then get file system mountpoints
        if len(blockdev_list) == 0:
            mount_list = self._iospec.get_fs_mountpoint_list()
            num_disks = len(mount_list)
        else:
            # If there are many disks, increase startup_timeout as needed
            # (only for block devices):
            num_disks = len(blockdev_list)
        startup_timeout = max(self.startup_timeout, 90 + (num_disks * 8))
        # logfile parser:
        parser_timeout = (self.interval * 2) + 30
        parser_desc = self._logging_prefix()
        self.parser = logfile_parser.FIOLogfileParser(
            self._client,
            self._output_file,
            desc=parser_desc,
            timeout=parser_timeout,
            startup_timeout=startup_timeout)
        # Install workload file, construct command-line:
        if not blockdev_list:
            self._execute_cmd = \
                self._install_fio_create_workload_build_cmd(
                    mount_point_list=mount_list)
        else:
            self._execute_cmd = \
                self._install_fio_create_workload_build_cmd(
                    blockdev_list=blockdev_list)
        # Do it:
        ret = super(FIO, self).start()
        # Make sure I/O actually starts successfully:
        self._wait_for_io_to_start(startup_timeout)
        # We're started!
        self._running = True
        return ret

    def _wait_for_io_to_start(self, startup_timeout):
        """
        We've launched the fio process; now wait for it to actually
        start up before returning
        """
        log_is_empty = True
        exitstatus = None

        def _get_stats_and_exitstatus():
            return self.parser.log_is_empty(), self.get_exitstatus()

        timeout = startup_timeout + (self.interval * 2)
        start_time = time.time()
        for log_is_empty, exitstatus in poll(_get_stats_and_exitstatus,
                                             interval=3, timeout=timeout):
            if not log_is_empty or exitstatus is not None:
                break
        # this is a timeout, empty log file, or exitstatus is None
        if log_is_empty:
            if exitstatus is None:
                cur_exit = self.get_exitstatus()
                msg = (
                    self._logging_prefix() +
                    ": IO seems to have stalled after "
                    "{} current exitstatus = {}".format(
                        time.time() - start_time, cur_exit))
            else:
                msg = (self._logging_prefix() + ": fio exited %d" % exitstatus)
            raise EnvironmentError(msg)

    def get_stats(self):
        """
        Method to get the most recent stats as reported by fio.
        Returns a dictionary of fio json output
        """
        success = False
        if self.parser is None:
            raise ValueError("get_stats called before I/O started")
        try:
            stats = self.parser.get_stats()
            success = True
        finally:
            if not success:
                self.cleanup = False
        return stats

    def check_for_errors(self):
        """
        Raises IOError if fio has encountered any I/O errors
        Raises EnvironmentError if fio has failed for any other reason
        """
        stats = self.get_stats()  # raises IOError if errors found in log
        if not stats:
            msg = ("%s: timed out waiting for fio stats" %
                   self._logging_prefix())
            self._should_run = False
            raise EnvironmentError(msg)
        for job in stats['jobs']:
            if job['error'] != 0:
                self._should_run = False
                raise EnvironmentError("%s: failed with error: %s" % (
                    self._logging_prefix(), job['error']))

    def _cleanup_remote_workloadfile(self):
        """ Call this after fio has completed; deletes remote workload file """
        if self.remote_workloadfile:
            self._client.run_cmd("rm -f -- " + self.remote_workloadfile)
            self.remote_workloadfile = None

    def _cleanup_remote_workdir(self):
        """
        Called after fio has completed successfully if self.cleanup is True
        """
        if self._remote_workdir:
            self._client.run_cmd("rm -rf -- " + self._remote_workdir)
            self._remote_workdir = None

    def _verify_io_happened(self):
        """ Raises IOError if no I/O has taken place """
        stats = self.get_progress()
        total_iops = 0
        total_iops += stats["total_read_iops"]
        total_iops += stats["total_write_iops"]
        total_iops += stats["total_trim_iops"]
        total_rate = 0
        total_rate += stats["total_read_rate"]
        total_rate += stats["total_write_rate"]
        total_rate += stats["total_trim_rate"]
        if total_iops == 0 and total_rate == 0:
            message = self._logging_prefix() + ": "
            message += ("After running the fio job, ZERO IO was performed, "
                        "current stats:\n{}".format(pformat(stats)))
            raise IOError(message)

    def get_progress(self, log_level=logging.INFO):
        """
        Will output to logger the progress on the IO object provided, will
        output:
            eta for completion of the current job
            total write rate of the current job
            total read rate of the current job.
        Args:
            log_level: log level to output info.
        Returns:
            io_dict which contains the information gathered:
            io_dict = {
                "locality": io_object._client.name,
                "eta" : the estimated time remaining for the io job,
                "total_read_rate" : The total read BW for the job
                "total_write_rate" : The total write BW for the job}
                "total_read_iops" : The total read BW for the job
                "total_write_iops" : The total write BW for the job}
        """
        io_dict = {
            "locality": self._client.name,
            "total_write_rate": 0,
            "total_write_iops": 0,
            "total_read_rate": 0,
            "total_read_iops": 0,
            "total_trim_iops": 0,
            "total_trim_rate": 0,
            "eta": 0}

        formatted_fio_result = self.get_stats()
        for job in formatted_fio_result["jobs"]:
            if int(formatted_fio_result["jobs"][0]["eta"]) > io_dict["eta"]:
                io_dict["eta"] = int(formatted_fio_result["jobs"][0]["eta"])
            if int(job["write"]["bw"]) > 1:
                io_dict["total_write_rate"] += int(job["write"]["bw"])
            if int(job["write"]["iops"]) > 1:
                io_dict["total_write_iops"] += int(job["write"]["iops"])
            if int(job["read"]["bw"]) > 1:
                io_dict["total_read_rate"] += int(job["read"]["bw"])
            if int(job["read"]["iops"]) > 1:
                io_dict["total_read_iops"] += int(job["read"]["iops"])
            if int(job["trim"]["iops"]) > 1:
                io_dict["total_trim_iops"] += int(job["trim"]["iops"])
            if int(job["trim"]["bw"]) > 1:
                io_dict["total_trim_rate"] += int(job["trim"]["bw"])

        eta_seconds = io_dict["eta"]
        message = ""
        # casing an overflow for datetime.timedelta
        if io_dict["eta"] < 86399999999999:
            eta_str = datetime.timedelta(seconds=eta_seconds)
        else:
            eta_str = "More than 2739726 years"
        message = "%s Estimated time to complete:%s " % (
            io_dict['locality'], eta_str)
        if io_dict["total_write_rate"] > 0:
            message += "write rate:%s/s, iops:%s " % (
                human_readable_size(io_dict["total_write_rate"] * 1024),
                io_dict["total_write_iops"])
        if io_dict["total_read_rate"] > 0:
            message += "read rate:%s/s, iops:%s " % (
                human_readable_size(io_dict["total_read_rate"] * 1024),
                io_dict["total_read_iops"])
        if io_dict["total_trim_iops"] > 0:
            message += "trim rate:%s/s, iops:%s " % (
                human_readable_size(io_dict["total_trim_rate"] * 1024),
                io_dict["total_trim_iops"])

        log.log(log_level, message)
        return io_dict
