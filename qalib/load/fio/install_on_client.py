# -*- coding: utf-8 -*-
"""
Code to install the fio binary onto a remote client
"""

import logging
import pkgutil
import uuid

__copyright__ = "Copyright 2020, Datera, Inc."

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def install_fio(client, version):
    """
    Returns a str, the remote path where the fio executable is located
    """
    if version is None:
        raise ValueError("An fio version must be specified")
    if client.os != "Linux":
        raise ValueError("fio requires a Linux client")

    out = client.run_cmd_check("uname -m")
    machine_type = out.strip()
    logger.debug("Client %s is machine type %s", client.name, machine_type)
    if machine_type == "ppc":
        logger.warn("Using hard-coded fio version for PPC clients")
        exename = "fio-2.2.8-ppc"
    elif machine_type == "x86_64":
        exename = "fio-" + version
    else:
        raise ValueError("Client %s unknown machine type %s" % (
                         client.name, machine_type))
    local_path = "assets/" + exename
    remote_path = "/tmp/" + exename

    status, _output = client.run_cmd("test -x " + remote_path)
    if status == 0:
        return remote_path  # it's already installed

    # Upload it to a temporary location on the client:
    random_str = unicode(uuid.uuid1()).replace('-', '')
    remote_tmppath = ".".join((remote_path, random_str))
    data = pkgutil.get_data(__name__, local_path)
    if not data:
        raise ValueError("Could not load package data %s" % local_path)
    with client.file_open(remote_tmppath, 'w') as tmpf:
        tmpf.write(data)
    client.run_cmd_check("chmod +x " + remote_tmppath)
    # Move it to the permanent location:
    client.run_cmd_check("mv -f -- " + remote_tmppath + " " + remote_path)
    logger.debug("Installed fio on client %s at %s", client.name, remote_path)
    return remote_path
