"""
Provides the IoComposite class, which controls multiple IO instances

Example:
    io1 = qalib.load.from_client_and_volume(client1, volume1,
                                            tool="fio_write_verify")
    io2 = qalib.load.from_client_and_volume(client2, volume2,
                                            tool="fio_write_verify")
    io_list = [io1, io2]
    io = qalib.load.composite.from_io_list(io_list)
    with io.start():
        ... IO is running on both clients, both volumes ...

"""
import datetime
import logging
import threading
import time

from .baseio import IoBase
from qalib.qabase.threading import Parallel
from qalib.qabase.formatters import human_readable_size
import qalib.equipment

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

__copyright__ = "Copyright 2020, Datera, Inc."

# TODO: do not sub-class IoBase; it is too concrete of an implementation
class IoComposite(IoBase):
    """
    Class which controls multiple IO objects (composite pattern)
    """

    def __init__(self, io_list, max_workers=None):
        """
        Parameter:
          io_list (list) - List of IO instances to manage
        """
        if not io_list:
            io_list = []
        self._io_list = io_list
        self.__io_has_been_started = False
        self.__io_has_been_stopped = False

        # todo come up with a better way to know what type of IOs they are
        if hasattr(self._io_list[0], "accesskey"):
            self._is_cosbench = True
            self.__max_workers = len(self._io_list)
        else:
            self._is_cosbench = False
            if max_workers is None:
                self.__max_workers = self._calc_max_workers()
            else:
                self.__max_workers = max_workers

        self._should_run = False
        self._thread = None

    @property
    def io_list(self):
        """
        Returns a list of io objects that the IO composite is comprised of,
        will return an empty list if None
        """
        if not self._io_list:
            return []
        else:
            return self._io_list[:]

    def _calc_max_workers(self):
        """ Return max num of thread workers: 3 =< num_clients <= 10"""
        equipment = qalib.equipment.get_default_equipment_provider()
        client_list = qalib.client.list_from_equipment(equipment,
                                                       required=False)
        num_clients = len(client_list)
        max_workers = max(5, min(num_clients, 10))
        log.debug(
            "max_workers for load.composite.start() : {}".format(max_workers))
        return max_workers

    def run_io_with_status(self, output_interval=120,
                           blocking_call=False):
        """
        Starts I/O. May be used as a context manager.
        Args:
            output_interval: (int) Number of seconds between output.
            blocking_call: (Bool) if True, don't return until:
                1.) IO error.
                2.) IO is done.
        """
        self.start()
        self._thread = threading.Thread(
            target=self._run_io_with_status,
            kwargs={"output_interval": output_interval})
        self._thread.daemon = True
        self._thread.start()
        # waiting for IO to start
        while not self.is_io_running() and self._should_run and \
                self.get_exitstatus() is None:
            time.sleep(1)
        if blocking_call:
            while self.is_io_running() and self.get_exitstatus() is None:
                time.sleep(5)
            self.check_for_errors()
            self.stop()
        else:
            return self

    def _run_io_with_status(self, output_interval):
        """Helper thread for reporting IO status and raising errors sooner"""
        timer_count = time.time()
        while self.get_exitstatus() is None and self._should_run:
            self.check_for_errors()
            if time.time() - timer_count > output_interval:
                self.get_progress()
                timer_count = time.time()
            else:
                time.sleep(2)
        self._running = False

    def start(self):
        """ Start all IO objects """
        if self.__io_has_been_started:
            raise ValueError("start() cannot be called more than once")
        fn_list = []
        for io in self._io_list:
            fn_list.append(io.start)
        success = False
        self._should_run = True
        try:
            Parallel(fn_list, max_workers=self.__max_workers).run_threads()
            success = True
        finally:
            if not success:
                self.stop()
        self.__io_has_been_started = True
        return self

    def stop(self):
        """ Stop all IO objects """
        fn_list = []
        for io in self._io_list:
            fn_list.append(io.stop)
        self._should_run = False
        if self._thread is not None:
            self._thread.join()

        Parallel(fn_list).run_threads()
        self.__io_has_been_stopped = True

    def check_for_errors(self):
        """ Checks for errors on all IO objects """
        for io in self._io_list:
            io.check_for_errors()

    def is_io_running(self, *args, **kwargs):
        """
        Returns True if I/O is running for any of the sub-I/O objects
        """
        for io in self._io_list:
            if io.is_io_running(*args, **kwargs):
                return True
        return False

    def get_exitstatus(self):
        """ Will return None until ALL IO has completed,
        If all IO has completed, will return non-zero if any IOs exited
        non-zero, else zero if all succeeded.
        """
        retval = None
        for io in self._io_list:
            exit_status = io.get_exitstatus()
            if exit_status is None:
                return None
            else:
                if retval is not None:
                    retval += exit_status
                else:
                    retval = exit_status
        return retval

    def get_progress(self, log_level=logging.DEBUG):
        """
         Will output to logger the progress on the IO object provided,

        Args:
            log_level: log level to output info.
        Returns:
        """
        if self._is_cosbench:
            raise RuntimeError("cosbench not supported by this toolkit")
        else:
            return self._get_fio_progress(log_level=log_level)

    def _get_fio_progress(self, log_level):
        """
        block aggregator for forward progress
            will output:
                eta for completion of the current job
                total write rate of the current job
                total read rate of the current job.
                total read iops of current job.
                total write iops of the current job.
        """
        io_dict = {
            "locality": "Cluster-wide",
            "eta": 0}
        check_list = ["total_write_rate", "total_write_iops", "total_read_iops",
            "total_read_rate", "total_trim_iops", "total_trim_rate",
            "read_bandwidth", "write_bandwidth"]
        for io_object in self.io_list:
            tmp_dict = io_object.get_progress(log_level=log_level)
            for key in check_list:
                if key not in io_dict:
                    io_dict[key] = 0
                if key in tmp_dict and tmp_dict[key] > 1:
                    io_dict[key] += tmp_dict[key]
            if "eta" in tmp_dict and tmp_dict["eta"] > io_dict["eta"]:
                io_dict["eta"] = tmp_dict["eta"]

        # casing an overflow for datetime.timedelta
        if io_dict["eta"] < 86399999999999:
            remaining = datetime.timedelta(seconds=io_dict["eta"])
        else:
            remaining = "More than 2739726 years"
        message = "%s\n Fio estimated time to complete:%s " % (
            io_dict['locality'], remaining)
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
        if io_dict["read_bandwidth"] > 0:
            message += "Object store read bandwidth:%s/s " % (
                human_readable_size(io_dict["read_bandwidth"]))
        if io_dict["write_bandwidth"] > 0:
            message += "Object store write bandwidth:%s/s " % (
                human_readable_size(io_dict["write_bandwidth"]))

        log.info(message)
        return io_dict

def from_io_list(iolist, max_workers=None):
    """
    Returns a composite object for managing a list of IO instances
    """
    if len(iolist) == 0:
        raise ValueError("Cannot instantiate composite with empty list")
    return IoComposite(iolist, max_workers=max_workers)
