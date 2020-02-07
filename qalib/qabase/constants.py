"""
This file holds all the constants, specification that are applicable across
the automation framework
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

# Byte sizes
KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB
PB = 1024 * TB

# Remote directory to stash fio output, etc on client systems
POSIXCLIENT_QA_LOGDIR = "/var/tmp/qalogs"

# Top-level directory to mount filesystem under on client systems
POSIXCLIENT_QA_MOUNT_PREFIX = "/tmp/workflow"
