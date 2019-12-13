# -*- coding: utf-8 -*-
"""
Setup logging

Used by qarunner and any stand-alone tools

Test cases should never use this module
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import gzip
import re
import shutil
import os
import time
import tempfile
import logging
import logging.handlers

__copyright__ = "Copyright 2020, Datera, Inc."


LOGFILE_FORMAT = '%(asctime)s %(name)s:%(levelname)s: %(message)s'
LOGFILE_SIZE = 4 * 1024**3 # 4 gigabytes
QATEST_LOGSCREEN_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
TOOL_LOGSCREEN_FORMAT = r'%(message)s'  # just like print
DIRMODE = int("01777", base=8)

def make_logdir(toplevelname="qatool_logs"):
    """
    Generates a newly-created log directory, using the current time
      e.g. "/tmp/qatest_results/20150201.1730.33.0"
    If the $WORK_DIRECTORY environment variable is set, create it under
    there.  If $WORKSPACE is set, use that.  Otherwise, use /tmp.
    """
    topleveldir = os.path.join(tempfile.gettempdir(), toplevelname)
    # /tmp/qatest_results/
    try:
        if not os.path.isdir(topleveldir):
            os.mkdir(topleveldir)
            try:
                os.chmod(topleveldir, DIRMODE)
            except (OSError, AttributeError, NotImplementedError):
                pass  # running on Windows, or not owner of directory
    except OSError:
        if os.path.isdir(topleveldir):
            pass  # EEXIST; another proc running in parallel beat us to it
        else:
            raise

    # /tmp/qatest_results/<datestamp>.<n>
    logdirbase = time.strftime("%Y%m%d.%H%M.%S")
    # there might be multiple test runs launched at the same time, and
    # the one-second granularity of the timestamp might not be enough,
    # so we add a unique number to the end of it.
    attempts = 0
    while True:
        logdirname = logdirbase + "." + str(attempts)
        logdir = os.path.join(topleveldir, logdirname)
        try:
            if not os.path.isdir(logdir):
                os.mkdir(logdir)  # mkdir is atomic; only succeeds for 1 proc
                try:
                    symlinkpath = os.path.join(topleveldir, "latest")
                    if os.path.exists(symlinkpath):
                        os.remove(symlinkpath)
                    os.symlink(logdirname, symlinkpath)
                except (AttributeError, NotImplementedError):
                    pass  # We're running on Windows; no symlink()
                except OSError:
                    pass  # Probably somebody beat us to it
                return logdir     # we got it
        except OSError:
            if os.path.isdir(logdir):
                pass  # EEXIST; somebody else beat us to the mkdir()
            else:
                raise
        attempts += 1
        if attempts > 10000:
            raise EnvironmentError("Failed to create results dir")

class CompressedFileHandler(logging.handlers.RotatingFileHandler):
    """
    A custom log handler to compress files.
    """

    def __init__(self, filename, mode='a', max_bytes=0, encoding=None, delay=0):
        """
        Note:
            We don't want to delete any log files for backupCount is
            automatically set to 0.

            If you really want to delete log files set backupCount to > 0
            example:
                handler = CompressedFileHandler('logfile.txt')
                handler.backupCount = 5

        Args:
            filename (str): Path of logfile.
            mode (str): Mode to open logfile in.
            max_bytes (int): Size of logfile before rotating. Defaults to 0.
                If max_bytes is equal to 0, rollover will not occur
            backup_count (int): Number of backups to keep. Defaults to 0
                If backup_count is equal to 0, all backups will be kept.
            encoding (str): Encoding to open logifle with.
            delay (bool): If delay is true file opening is delayed until first
                call to emit().
        """
        backup_count = 0

        super(CompressedFileHandler, self).__init__(filename, mode=mode,
                                                    maxBytes=max_bytes,
                                                    backupCount=backup_count,
                                                    encoding=encoding,
                                                    delay=delay)
        self.suffix = "%Y%m%d-%H%M%S"
        self.extMatch = r"^\d{4}\d{2}\d{2}-\d{2}\d{2}\d{2}$"
        self.extMatch = re.compile(self.extMatch)

    def doRollover(self):
        """
        Do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  If there is a backup count, then we have to
        get a list of matching filenames, sort them and remove the one with the
        oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        timeTuple = time.gmtime(currentTime)
        dfn = '{}.{}.gz'.format(self.baseFilename, time.strftime(self.suffix,
                                                                 timeTuple))
        if os.path.exists(dfn):
            os.remove(dfn)
        # compress with gzip
        if os.path.exists(self.baseFilename):
            with open(self.baseFilename, 'rb') as f_in,\
                 gzip.open(dfn, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(self.baseFilename)
        # We should never be deleting any files
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        self.stream = self._open()

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + '.'
        suffix = '.gz'
        plen = len(prefix)
        slen = len(suffix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                date = fileName[plen:-slen]
                if self.extMatch.match(date):
                    result.append(os.path.join(dirName, fileName))
        result.sort()
        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]
        return result
