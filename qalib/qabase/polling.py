# -*- coding: utf-8 -*-
"""
Location for polling functions
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import logging
from time import time, sleep

from qalib.qabase.exceptions import PollingException

LOG = logging.getLogger(__name__)


def poll(function, args=None, kwargs=None, retries=None, interval=None,
         timeout=None, exception=None, inspection=None):
    """
    Generic polling function that accepts a function as an argument and
    polls that function with the provided arguments until the retries are
    exhausted.  Arguments for the provided function should be placed in the
    args/kwargs parameters as a list/dictionary respectively.  These will be
    forwarded by the polling function to the polled function.

    Example usage:

        def example_check():
            gen = poll(some_func, args=[arg1, arg2], kwargs={'kwarg1': value1,
            'kwarg2': value2}, retries=3, interval=2)

            for result in gen:
                if result == what_i_want:
                    return
            raise ValueError("The poll didn't find what we're looking for")

    Exception Example usage:

        def poll_this_func():
            i = random.randint(0, 10)
            if i % 2 == 0
                raise SpecialException("This exception is special")
            else:
                return i

        def exception_check():
            gen = poll(poll_this_func, exception=SpecialException,
                       inspection="special")

            for result in gen:
                if result == 5:
                    return
            raise ValueError("We never found what we were looking for, but
                             the poll never raised SpecialException")

    :param function: The function to poll
    :param args: The positional args to pass to the function
    :param kwargs: The keyword args to pass to the function
    :param retries: The maximum number of retries to poll for before stopping
    :param interval: The minimum interval between poll attempts (no
    guaranteed maximum interval since it depends on when the polled
    function returns)
    :param timeout: Maximum amount of time to poll (in seconds).  ##IF THIS
    PARAMETER IS PROVIDED, THE RETRIES PARAMETER IS IGNORED.##
    :param exception: Exception or tuple of Exceptions to suppress during
    polling
    :param inspection: String used to match against Exception message.  If
    provided, only Exceptions containing this string will be suppressed

    :returns: A generator of the result of the function being polled as called
    with the provided arguments each polling attempt.
    """
    if not kwargs:
        kwargs = {}
    if not args:
        args = []
    # Flag a common (and confusing) user error:
    if isinstance(args, str) or isinstance(args, unicode):
        raise ValueError("Error: args must be an array, not a string")

    # Make sure exceptions lists passed in are converted to tuples
    # Catching them won't work as a list
    if exception and type(exception) == list:
        exception = tuple(exception)

    if interval is None:
        LOG.warn("poll interval param ought to be specified")
        interval = 1
        # raise ValueError("Must specify poll interval")

    if timeout is None and retries is None:
        LOG.warn("poll timeout or retries param ought to be specified")
        retries = 20
        # raise ValueError("Must specify poll timeout or retries")

    if timeout:
        retries = int(timeout / interval)
    else:
        # in the case where timeout is None but retries and interval are
        # provided
        timeout = 1000000

    current = 0
    start_time = time()

    while (current < retries) and ((time() - start_time) < timeout):
        # Suppress indicated exceptions and re-raise them if the inspection
        # field is found in the error message
        if exception:
            try:
                yield function(*args, **kwargs)
            except exception as e:
                if inspection and inspection not in e[0]:
                    raise
                else:
                    yield None
        else:
            yield function(*args, **kwargs)

        current += 1
        sleep(interval)


def result_poll(function, expected_result, args=None, kwargs=None,
                retries=20, interval=1, timeout=None, callback_dict=None):
    """
    A wrapper for `poll` that allows the caller to provide a condition to
    check for each poll result.  The function will return on the first
    encounter where the poll result == expected_result parameter.

    :param function: The function to poll
    :param expected_result: The result to try and match against the result
    from the polled function
    :param args: The positional args to pass to the function
    :param kwargs: The keyword args to pass to the function
    :param retries: The maximum number of retries to poll for before stopping
    :param interval: The minimum interval between poll attempts (no
    guaranteed maximum interval since it depends on when the polled
    function returns)
    :param timeout: Maximum amount of time to poll (in seconds).  ##IF THIS
    PARAMETER IS PROVIDED, THE RETRIES PARAMETER IS IGNORED.##
    :return: Total time required to reach the expected_result state.

    :param callback_dict: A dict data structure that indicates a success
    function and/or a failure function to be executed on success and/or failure
    respectively.

        Here is an example of the structure:

            callback_dict = {
                "success": {
                    "func": somesuccessfunc,    # Required if parent key exists
                    "args": (a, tuple, of, args)
                    "kwargs": {
                        "a": 1,
                        "dict": 2,
                        "of": 3,
                        "kwargs": 4
                    }
                }
                "failure": {
                    "func": somefailfunc,       # Required if parent key exists
                    "args": (a, tuple, of, args)
                    "kwargs": {
                        "a": 1,
                        "dict": 2,
                        "of": 3,
                        "kwargs": 4
                    }
                }
            }
        The results of the success function and/or the failure function will
        be logged to the DEBUG logger.
                            ##### BEWARE #####
        ##### DO NOT USE THIS TO CHANGE STATE, JUST OBSERVE IT #####

    :raises PollingException: If the end of the polling period is reached
    without encountering a matching result from the polled function.
    """
    cnt = 0
    for result in poll(function,
                       args,
                       kwargs,
                       retries,
                       interval,
                       timeout=timeout):
        cnt += 1
        if result == expected_result:

            # Call success function in callback_dict if it exists
            if callback_dict and callback_dict.get("success"):
                LOG.debug(str(callback_dict["success"]["func"](
                    *callback_dict["success"].get("args", ()),
                    **callback_dict["success"].get("kwargs", {}))))

            return cnt * interval

    # Call failure function in callback_dict if it exists
    if callback_dict and callback_dict.get("failure"):
        LOG.debug(str(callback_dict["failure"]["func"](
            *callback_dict["failure"].get("args", ()),
            **callback_dict["failure"].get("kwargs", {}))))

    raise PollingException(
        "Expected Result: '{}' not found for function: '{}' after '{}' sec."
        " args: {} , kwargs {}"
        .format(expected_result, function, str(cnt * interval),  args, kwargs))
