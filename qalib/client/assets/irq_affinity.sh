#!/usr/bin/env bash
#!/bin/bash

set_irq_affinity()
{
    local cpu startcpu incrcpu
    local dev
    local irqs irq
    local NCPUS

    dev="$1"
    startcpu="$2"
    incrcpu="$3"

    NCPUS=$(grep -c ^processor /proc/cpuinfo)
    cpu="$startcpu"
    irqs=$(cat < /proc/interrupts | grep "$dev" | awk '{print $1}' |
	   sed -e 's/://')
    for irq in $irqs
    do
	echo "$cpu" > /proc/irq/"$irq"/smp_affinity_list
	cpu=$((cpu + incrcpu))
	cpu=$((cpu%NCPUS))
    done
}

set_affinity_for_backend_network()
{
    local ethdevs dev mellanoxCard frontpci
    local pcilink fnetcard
    local DEVICE
    local MELLANOXRDMA

    # set the RDMA affinity. Mellanox cards have
    # 4 Queues and we might want separate it out
    # across 2 CPUS if needed.
    MELLANOXRDMA="mlx4-"
    set_irq_affinity "$MELLANOXRDMA" 0 2

    # We expect that network backend is going to be a Mellanox card and
    # hence use that as as backend interface.
    ethdevs=$(ls /sys/class/net)
    mellanoxPci=$(lspci | grep -i ConnectX-3 | awk ' { print $1 } ')
    for mellanoxCard in $mellanoxPci
    do
	for dev in $ethdevs
	do
	    pcilink=$(readlink /sys/class/net/"$dev"/device |
		      grep 0000:"$mellanoxCard")
	    if [ ! -z "${pcilink}" ]; then
		DEVICE="$dev-"
#                ifconfig "$dev" up
		set_irq_affinity "$DEVICE" 0 1
		echo "set_irq_affinity "$DEVICE" 0 1"
		# VLAN/MTU will be handled as part of cluster
		# init, so do not set MTU here
		# ifconfig "$dev" mtu 4096
	    fi
	done
    done
}

set_affinity_for_backend_network
