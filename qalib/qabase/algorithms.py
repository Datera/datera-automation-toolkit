# -*- coding: utf-8 -*-
"""
Library for useful algorithms not available in the normal python libraries
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import random


def round_robin(worker_list, target_list, randomize=True,
                no_empty=False):
    """
    Returns a dictionary mapping workers to targets
    Parameters:
      worker_list (list) - list of any hashable objects
      target_list (list) - list of any objects
      randomize (bool) - If False, the return value is the same every time.
                         If True, the workers are mapped to targets in
                         semi-random order.
      no_empty (bool) - If True, require all workers to be used
    A typical use case would be distributing volumes to client systems for
    running I/O.

    Example: More targets than workers:
      >>> round_robin(['c_1', 'c_2'], ['vol_1', 'vol_2', 'vol_3'],
                      randomize=False)
      {'c_1': ['vol_1', 'vol_3'], 'c_2': ['vol_2']}

    Example: Fewer targets than workers (not all workers will be used):
      >>> round_robin(['c_1', 'c_2', 'c_3'], ['vol_1', 'vol_2'],
                      randomize=False)
      {'c_1': ['vol_1'], 'c_2': ['vol_2'], 'c_3': []}
    """
    if len(worker_list) == 0:
        raise ValueError("Error: worker_list cannot be empty")
    if no_empty and len(target_list) < len(worker_list):
        raise ValueError("Length of worker_list must be >= target_list unless"
                         " no_empty is set to False")
    if randomize:
        worker_list = list(worker_list)  # shallow copy
        random.shuffle(worker_list)
    ret = {}
    for worker in worker_list:
        ret[worker] = []
    num_workers = len(worker_list)
    for index, target in enumerate(target_list):
        worker = worker_list[index % num_workers]
        ret[worker].append(target)
    return ret
