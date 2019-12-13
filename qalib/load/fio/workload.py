"""
Creates an FIO config file

This module should be considered package-private
"""
from __future__ import (unicode_literals, print_function)
import logging
from pprint import pformat

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())


def override_default_workload_params(default_params, user_params):
    """
    Reconciles default FIO parameters with incompatible user-specified
    parameters.
    Returns a dict.
    Example:
      >>> default_params = {"bssplit": "4k/10:8k/90",
      ...                   "iodepth": "16", "numjobs": "1"}
      >>> user_params = {"bs": "1M"}
      >>> override_default_workload_params(default_params, user_params)
      {'bs': '1M', 'iodepth': '16', 'numjobs': '1'}
    """
    if default_params is None:
        default_params = {}
    if user_params is None:
        user_params = {}
    if not user_params:
        return default_params

    # Modify the default params to remove anything which conflicts with
    # what the user wants
    default_params = default_params.copy()  # modifiable copy
    if 'bs' in user_params:
        for conflicting_param in ['bssplit', 'bsrange']:
            if conflicting_param in default_params:
                default_params.pop(conflicting_param)
    if 'bssplit' in user_params:
        for conflicting_param in ['bs', 'bsrange']:
            if conflicting_param in default_params:
                default_params.pop(conflicting_param)
    if 'bsrange' in user_params:
        for conflicting_param in ['bssplit', 'bs']:
            if conflicting_param in default_params:
                default_params.pop(conflicting_param)

    workload_params = default_params.copy()
    workload_params.update(user_params)
    return workload_params

def generate_fio_workload(workload_params, blockdev_list=None,
                          mount_point_list=None, force_jobname=None,
                          force_one_job=False):
    """
    Generate a workload file
    """
    if workload_params is None:
        workload_params = {}
    else:
        workload_params = workload_params.copy()  # modifiable copy

    if not blockdev_list and not mount_point_list:
        raise AssertionError(
            "No block dev list or mount_point_list provided to generate workload")

    _validate_workload_params(workload_params)

    # Global section
    workload = '[global]\n'
    workload += "group_reporting\n"
    workload += "direct=%d\n" % int(workload_params.pop("direct", 1))
    workload += "random_generator=%s\n" % (
        workload_params.pop("random_generator", "tausworthe64"))
    workload += "ioengine=%s\n" % workload_params.pop("ioengine", "libaio")
    workload += "\n"

    if force_one_job:
        if force_jobname:
            jobname = force_jobname
        else:
            jobname = "forced_one_job"
        jobname = _sanitize_jobname(jobname)
        if "offset_list" in workload_params:
            msg = "Cannot use offset_list and force_one_jobname together"
            raise ValueError(msg)
        workload += _generate_fio_job_block(jobname=jobname,
                                            blockdev=blockdev_list,
                                            job_params=workload_params)
    else:
        # Per-device sections
        for blockdev in blockdev_list:
            # Job name
            if force_jobname:
                jobname = force_jobname
            else:
                # Use the device file name as the job name
                # TODO: use volume UUID if available; get this from iospec
                jobname = blockdev
            jobname = _sanitize_jobname(jobname)
            if "offset_list" in workload_params:
                offset_list = workload_params.pop('offset_list')
            else:
                offset_list = None

            if offset_list is not None:
                for offset in offset_list:
                    per_offset_jobname = jobname + "-offset_" + str(offset)
                    # Per-device per offset section
                    workload += _generate_fio_job_block(blockdev=blockdev,
                        jobname=per_offset_jobname, job_params=workload_params,
                        offset=offset)
            else:
                # Per-device section
                workload += _generate_fio_job_block(
                    blockdev=blockdev, jobname=jobname,
                    job_params=workload_params)

    log.debug(workload)
    return workload

def _raise(workload_params, msg):
    message = str(msg)
    message += ("\n The parameters that were passed into fio workload "
                "generator:\n {}".format(pformat(workload_params)))
    raise ValueError(message)

def _sanitize_jobname(jobname):
    """ e.g. '/dev/sdb' -> 'dev_sdb' """
    jobname = jobname.replace("/", "_")
    if jobname.startswith("_dev_"):
        jobname = jobname[5:]
    return jobname

def _generate_fio_job_block(jobname=None, blockdev=None,
                            job_params=None,
                            offset=None):
    """
    Generate one block/stanza of an fio workload file
    """
    if job_params is None:
        job_params = {}
    else:
        job_params = job_params.copy()  # modifiable local copy

    workload = ""
    workload += '[%s]\n' % jobname
    if type(blockdev) == list:
        for dev in blockdev:
            workload += "filename=" + dev + "\n"
    else:
        workload += "filename=" + blockdev + "\n"
    if offset is not None:
        workload += "offset=" + str(offset) + "\n"
    for key, val in job_params.iteritems():
        if val is not None and val != '':
            workload += "=".join([key, str(val)])
        else:
            workload += str(key)
        workload += "\n"
    workload += "\n"
    return workload

def _validate_workload_params(workload_params):
    """
    This is intended to make sure that incompatible options aren't specified
    together.  If the options won't work, it will raise.  If the options
    provided may cause a problem, a warning message should be logged.
    """
    if workload_params is None:
        workload_params = {}
    if "bs" not in workload_params and "bssplit" not in workload_params \
            and "bsrange" not in workload_params:
        msg = "No variation of block size passed into fio."
        _raise(workload_params, msg)

    if ("bs" in workload_params and "bssplit" in workload_params) or (
            "bs" in workload_params and "bsrange" in workload_params) or (
            "bssplit" in workload_params and "bsrange" in workload_params):
        msg = ("Only one parameter form bs, bssplit, and bsrange can be "
               "provided to fio at a time.")
        _raise(workload_params, msg)

    if "iodepth" not in workload_params:
        msg = "No iodepth passed into fio."
        _raise(workload_params, msg)

    if "numjobs" in workload_params:
        if int(workload_params["numjobs"]) > 16:
            log.warn("More jobs specified than 16 per device, this may or "
                     "may not be intentional.  Please verify your intentions "
                     "with fio.")

    if "random_distribution" in workload_params and "rand" not in \
            workload_params["rw"]:
        log.warn("Random distribution algorithm selected without a random "
                 "workload, this will not have any affect.")

    if "buffer_compress_percentage" not in workload_params:
        log.warn("Buffer compress percentage not provided for fio, this may "
                 "cause unexpected results with "
                 "compression.")

    if "direct" not in workload_params:
        log.warn("direct not provided for fio, this may cause "
                 "unexpected results because IO's may not be "
                 "completed at the storage layer.")

def generate_fio_workload_with_mountpoint(workload_params,
                          mount_point_list=None, force_jobname=None,
                          force_one_job=False):
    """
    Generate a workload file
    """
    if workload_params is None:
        workload_params = {}
    else:
        workload_params = workload_params.copy()  # modifiable copy

    if not mount_point_list:
        raise AssertionError(
            "No mount_point_list provided to generate workload")

    _validate_workload_params(workload_params)

    # Global section
    workload = '[global]\n'
    workload += "group_reporting\n"
    workload += "direct=%d\n" % int(workload_params.pop("direct", 1))
    workload += "random_generator=%s\n" % (
        workload_params.pop("random_generator", "tausworthe64"))
    workload += "ioengine=%s\n" % workload_params.pop("ioengine", "libaio")
    workload += "\n"

    # Per-mount sections
    for mount in mount_point_list:
        # Use the mount file name as the job name
        # TODO: use volume mount if available; get this from iospec
        jobname = mount
        jobname = _sanitize_jobname(jobname)
        workload += _generate_fio_job_mountpoint(
            mount_point=mount, jobname=jobname,
            job_params=workload_params)
    log.debug(workload)
    return workload

def _generate_fio_job_mountpoint(jobname=None, mount_point=None,
                                 job_params=None,
                            offset=None):
    """
    Generate one block/stanza of an fio workload file
    """
    if job_params is None:
        job_params = {}
    else:
        job_params = job_params.copy()  # modifiable local copy

    workload = ""
    workload += '[%s]\n' % jobname
    if type(mount_point) == list:
        temp = ""
        for mnt in mount_point:
            temp += mnt
            temp += ":"
        temp = temp[:-1]
        job_params["directory"] = temp
    else:
        job_params["directory"] = mount_point
    if offset is not None:
        workload += "offset=" + str(offset) + "\n"
    for key, val in job_params.iteritems():
        if val is not None and val != '':
            workload += "=".join([key, str(val)])
        else:
            workload += str(key)
        workload += "\n"
    workload += "\n"
    return workload
