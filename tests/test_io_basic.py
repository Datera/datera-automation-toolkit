#!/usr/bin/env python
# -*- coding: utf-8 -*-

import qatest
import qalib.api
import qalib.load
import qalib.limitsutil
import qalib.clusterutil
import qalib.whitebox

from qalib.qabase.constants import GB
from qalib.testbedsetup import TestBedSetup

__copyright__ = "Copyright 2020, Datera, Inc."

def get_setup_dict_from_cluster(cluster=None, single_volume=False):
    """
    Helper function that normalizes the setup dictionary for testbed setups
    based on the cluster provided.
    Args:
        single_volume(Bool): Will ensure only one volume is created if specified
    """
    num_nodes_in_cluster = len(qalib.whitebox.list_from_cluster(
        clusterequipment=cluster))
    volume_size_bytes = 100 * GB
    if single_volume:
        num_si_per_ai = 1
    else:
        num_si_per_ai = num_nodes_in_cluster * 2
    if num_nodes_in_cluster < 3:
        num_replicas = 1
    else:
        num_replicas = 3
    setup_dict = {"num_app_instances": 1,
                  "num_storage_instance_per_ai": num_si_per_ai,
                  "volumes_per_si": 1,
                  "volume_size": volume_size_bytes,
                  "num_replicas": num_replicas}
    return setup_dict

class TestCase(qatest.TestCase):
    """
    Insanity tests making sure that basic failure scenarios pass
    """
    def setUp(self):
        self.api = qalib.api.sdk_from_cluster(self.cluster)
        self.cluster_util = qalib.clusterutil.from_cluster(self.cluster)

        # get limits based on release
        self.limits = qalib.limitsutil.from_api(api=self.api)

        self.tbs = None

    def tearDown(self):
        if self.tbs is not None:
            self.tbs.cleanup()

    def test_insanity_run_io(self):
        """Basic test that configures volumes and runs IO with verification"""
        self.logger.info("Configuring 20 pct of the cluster")
        setup_dict = get_setup_dict_from_cluster(cluster=self.cluster)
        self.tbs = TestBedSetup.from_dict(config_dict=setup_dict,
                                          equipment=self.equipment)
        self.tbs.configure()

        io = qalib.load.from_tbs(self.tbs, tool="fio_write_verify",
                                 iodepth=1,
                                 do_verify="1",
                                 verify_backlog=1000,
                                 runtime="120")
        self.logger.info("Running write verify workload for 2 minutes.")
        io.run_io_with_status(output_interval=10, blocking_call=True)
        self.logger.info("Running IO completed successfully. TEST PASS")
