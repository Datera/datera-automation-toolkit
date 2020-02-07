# datera-automation-toolkit

An external quality assurance toolkit.

# Pre-requisites before beginning testing

1.  Cluster not deployed.
2.  Datera nodes installed running Datera OS.
3.  At least one "client" or head node with 2 high-speed TCP/IP connections to the Datera nodes running CentOS7.5.
4.  External NTP.
5.  Two /25 networks for front end traffic.
6.  Two /25 networks for back end traffic.
7.  Clients must have access to `yum` repositories, or communicate a way to install distribution packages, such as iscsi, multipath, etc. The packages will be installed as part of the cluster deploy process: `iscsi-initiator-utils` `sg3_utils` `sysstat` `fio` `lsscsi` `python-pip` `curl`
8.  Execution host for python tests must have access to common python packages and pip repositories.
9.  Execution host must have `sshpass`, `rsync`, and `wget`
10. Two json description files. You may call these whatever you want but they must end in `.json`
    -   equipment description: everything needed to test a cluster. See `sample/equipment.json` for an example of what this looks like
    -   cluster init description: everything needed to deploy a cluster. See `sample/init.json` for an example of what this looks like.

# Installation & Usage

All commands below should be run from the directory on the executor where the toolkit has been unpacked or cloned.


## Install python dependencies on executor

    pip install -r requirements.txt


## Deploy cluster

    python deploy_cluster.py -c path/to/equipment/description.json path/to/init/description.json

The `deploy_cluster` script will log to a textfile in the current directory. The filename will be prefixed with "datera\_cluster\_deploy-", eg `datera_cluster_deploy-1581051882.txt`.



## Verify that logs can be collected from cluster

It's recommended to you first run the "logcollect" test to ensure that logs can be gathered from the clients & storage nodes in the event of test failures.

    python run_tests.py -c path/to/equipment/description.json --test-logcollect


## Begin testing

    python run_tests.py -c path/to/equipment/description.json
