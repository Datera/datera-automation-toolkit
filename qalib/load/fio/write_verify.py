"""
A workload designed to write a pattern and read it back later
 (or while it is written.)
"""

from qalib.load import FIO

__copyright__ = "Copyright 2020, Datera, Inc."

class FIOWriteVerify(FIO):

    def __init__(self, iospec, **kwargs):
        workload_params = {
            "iodepth": "16",
            "numjobs": "1",
            "loops": "1",
            "direct": "1",
            "buffer_compress_percentage": "50",
            "refill_buffers": "1",
            "bssplit": "4k/10:8k/10:16k/10:32k/10:64k/30:128k/10:256k/20",
            "rw": "randwrite",
            "do_verify": "0",
            "verify_interval": "512",
            "verify": "crc32c-intel",
            "dedupe_percentage": "50",
            "buffer_compress_chunk": "4k"
            }
        workload_params = \
            self._override_default_workload_params(workload_params, kwargs)
        super(FIOWriteVerify, self).__init__(iospec, **workload_params)
