#!/usr/bin/env python
"""
Provides the FIOLogfileParser class
"""
import simplejson as json
import re
import threading
import time

class FIOLogfileParser(object):
    """ Parses an fio JSON logfile on a remote system """

    def __init__(self, client, filepath, desc=None, timeout=30,
                 startup_timeout=120):
        """
        Parameters:
          client (qalib.client.Client)
          filepath (str) - FIO logfile on client
          desc (str) - details about fio, used in error messages
          startup_timeout (int) - how long to wait for fio to start producing
                 logs when it first starts up
          timeout (int) - how long to wait for fio to start produce new logs
        """
        self._output_file_handle = client.file_open(filepath, 'r')
        self._latest_rawdatablob = None
        self._latest_parsed_stats_output = None
        self._lock = threading.Lock()
        self._bufsize = 1024 * 1024  # Chunk size for file read()s
        if desc:
            self._desc = desc
        else:
            self._desc = "fio parser"
        self._timeout = timeout
        self._startup_timeout = startup_timeout

    def _check_for_errors(self, datablob):
        """
        Raises IOError if the given string contains an fio error message
        """
        # fio can occasionally give garbage meaningless output, catching it here
        whitelist = ["stat: No such file or directory"]
        errormsgs = re.findall(r'^([^{}\s].*$)', datablob, re.MULTILINE)
        for error in errormsgs:
            if error in whitelist:
                errormsgs.remove(error)
        if errormsgs:
            msg = self._desc + ":\n" + '\n'.join(errormsgs)
            raise IOError(msg)

    def _get_next_rawdatablob(self):
        """
        Returns the next blob, as a str
        Returns None if no new data
        Raises an exception if any error messages are encountered

        This method is responsible for noticing if fio reports an I/O error,
        so if you modify this code, please be sure that fio errors are not
        missed!
        """
        # remember where we were:
        orig_offset = self._output_file_handle.tell()

        # read in new data in chunks:
        datablob = ''
        while True:
            readbuf = self._output_file_handle.read(self._bufsize)
            if not readbuf:
                # We reached EOF, but no end of blob yet, so rewind the
                # file handle to where it was and return None
                self._output_file_handle.seek(orig_offset)
                # Look for error messages:
                self._check_for_errors(datablob)
                return None
            datablob += readbuf

            # See if we've reached the end of a blob:
            try:
                end_pointer = datablob.index("\n}") + 2
            except ValueError:
                continue  # nope, not yet; keep looking
            datablob = datablob[:end_pointer]
            # position the file pointer at the end of this blob:
            self._output_file_handle.seek(orig_offset + len(datablob))
            # Look for error messages:
            self._check_for_errors(datablob)
            return datablob

    def _get_last_rawdatablob(self):
        """
        Keep reading new blobs until we reach the last one
        Returns None if no data in the file yet
        """
        while True:
            next_blob_data = self._get_next_rawdatablob()
            if not next_blob_data:
                break
            self._latest_rawdatablob = next_blob_data
        return self._latest_rawdatablob

    def log_is_empty(self):
        """
        Returns True if the logfile is still empty (fio still starting)
        Returns False if the logfile contains any data
        """
        with self._lock:
            if self._output_file_handle is None:
                raise ValueError("Logfile is already closed")
            if self._output_file_handle.tell() != 0:
                return False
            readbuf = self._output_file_handle.read(self._bufsize).strip()
            self._output_file_handle.seek(0)
            if readbuf:
                return False
            return True

    def get_stats(self):
        """
        Gets the most recent stats as reported by fio.
        Returns a dictionary of fio json output

        Raises IOError if any fio error messages are detected
        """
        with self._lock:
            # If we're already closed, just return the last stats:
            if not self._output_file_handle:
                return self._latest_parsed_stats_output

            if not self._latest_parsed_stats_output:
                timeout = self._startup_timeout
            else:
                timeout = self._timeout

            # fio can be a bit slow to get started, so poll if needed for
            # the initial stats blob to appear
            end_time = time.time() + timeout
            newest_blob_data = None
            while time.time() < end_time and newest_blob_data is None:
                newest_blob_data = self._get_last_rawdatablob()
                if not newest_blob_data:
                    time.sleep(1)

            if not newest_blob_data:
                msg = self._desc + ": timed out waiting for fio to start"
                raise EnvironmentError(msg)

            try:
                parsed_stats_output = json.loads(newest_blob_data)
            except ValueError:
                # this should never happen
                msg = self._desc + ": cannot parse FIO log:\n"
                msg += repr(newest_blob_data)
                raise EnvironmentError(msg)

            self._latest_parsed_stats_output = parsed_stats_output
            return self._latest_parsed_stats_output

    def get_all_stats(self):
        """
        Returns *all* the blobs in the file
        Note: incomplete data at the end is skipped
        Warning: this is potentially very slow
        """
        orig_offset = None
        with self._lock:
            # Remember where we are in the file:
            if self._output_file_handle:
                orig_offset = self._output_file_handle.tell()
                # setting file offset to beginning of file.
                self._output_file_handle.seek(0)
            try:
                stats_list = []
                while True:
                    next_blob_data = self._get_next_rawdatablob()
                    if not next_blob_data:
                        break
                    try:
                        parsed_stats_output = json.loads(next_blob_data)
                    except ValueError:
                        # this should never happen
                        msg = self._desc + ": cannot parse FIO log:\n"
                        msg += repr(next_blob_data)
                        raise EnvironmentError(msg)
                    stats_list.append(parsed_stats_output)
                return stats_list
            finally:
                if orig_offset is not None:
                    # Go back to where we were:
                    self._output_file_handle.seek(orig_offset)

    def close(self):
        with self._lock:
            if self._output_file_handle:
                self._output_file_handle.close()
                self._output_file_handle = None
