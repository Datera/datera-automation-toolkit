# -*- coding: utf-8 -*-
"""
Location for QA Automation Exceptions
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import functools
# Exception definitions:
import socket
import paramiko
import xmlrpclib

from dfs_sdk.exceptions import (
    ApiAuthError,
    ApiConflictError,
    ApiConnectionError,
    ApiError,
    ApiInternalError,
    ApiInvalidRequestError,
    ApiNotFoundError,
    ApiTimeoutError,
    ApiUnavailableError,
    ApiValidationFailedError,
    SdkEndpointNotFound,
    SdkEntityNotFound,
    SdkError,
    SdkTypeNotFound,
    _ApiResponseError as ApiResponseError)
import unittest as _unittest

__copyright__ = "Copyright 2020, Datera, Inc."


class PollingException(AssertionError):
    """
    Raised when certain polling wrapper functions reach the end of their
    polling durations.
    """
    pass

###############################################################################

class DiskCapacityException(Exception):
    """
    Raised when disk capacity does not changes after a certain time period.
    """
    pass

class ApiNotSupportedError(ApiError, _unittest.SkipTest):
    """ API version not supported """
    pass


###############################################################################

class ConnectionError(EnvironmentError):
    """ SSH connection error """
    pass


class SubConnectionError(ConnectionError):
    """ Workaround connection error """
    pass


class CliError(Exception):
    """ CLI error """
    pass


class BlockerError(Exception):
    """
    A lethal error that we want to stop all tests waiting in the queue
    and freeze the current state of the cluster
    """
    pass


class FeatureNotSupportedError(Exception):
    """
    An error due to feature or library call not valid in the product
    """
    pass

##############################################################################


class EquipmentNotFoundError(_unittest.SkipTest):
    '''
    This exception is raised when the EquipmentProvider is asked for
    equipment which it does not have.
    '''
    pass


##############################################################################

class _GuiError(Exception):
    """ Datera Base Exception Class For GUI
    """

    def __init__(self, msg=None, screen_shot_path=None, stacktrace=None):
        self.msg = msg
        self.screen_shot_path = screen_shot_path
        self.stacktrace = stacktrace

    def __str__(self):
        exception_msg = "\nError Message: %s\n" % self.msg
        if self.screen_shot_path is not None:
            exception_msg += (
                "Find Screenshot At: %s\n" % self.screen_shot_path)
        return exception_msg


class DateraGuiError(_GuiError):
    pass


class GuiResponseError(_GuiError):
    """ ErrorInResponseException: Error due to an error has occurred on the
    server side This may happen when communicating with the firefox extension
    or the remote driver server.
    """
    def __init__(self, response, msg, screen_shot_path=None):
        super(_GuiError, self).__init__(self, msg=msg,
                                        screen_shot_path=screen_shot_path)
        self.response = response


class GuiElementNotFoundError(_GuiError):
    """ NoSuchElementException: Error due to element not found on GUI """
    pass


class GuiElementNotSelectableError(_GuiError):
    """ ElementNotSelectableException: Error due to element is disabled on GUI
    """
    pass


class GuiElementNotVisibleError(_GuiError):
    """ ElementNotVisibleException: Error due to element is not visible but
    present on GUI
    """
    pass


class GuiElementNotClickable(_GuiError):
    """ ElementNotClickableException: Error due to element is not clickable but
    present on GUI
    """
    pass


class GuiElementDetachedFromDOMError(_GuiError):
    """ StaleElementReferenceException: Error due to element is not longer
    appearing or attached to DOM (Element is removed from the GUI)
    """
    pass


class GuiElementAttributeNotFoundError(_GuiError):
    """ NoSuchAttributeException: Error due to attribute of element could not
    be found.
    """
    pass


class GuiAlertNotFoundError(_GuiError):
    """ NoAlertPresentException: Error due to no alert present on GUI or while
    switching to alert
    """
    pass


class TimeoutError(_GuiError):
    """ TimeoutException: Timeout waiting for a resource to be created """
    pass


class InvalidElementLocator(_GuiError):
    """ Error if locator passed is in invalid format.
        Valid formats are - "class=*"
                            "css=*"
                            "id=*"
                            "name=*"
                            "xpath=*"
                            "link_text=*"
                            "partial_link_text=*"
                            "tag_name=*"
    """
    pass


def with_exception_translation(function):
    """
    Decorator to translate various lower-level exceptions into exceptions
    published by this package's API.
    """
    @functools.wraps(function)
    def wrapper_function(*args, **kwargs):
        """ Call the original function, translate exceptions if needed """
        try:
            return function(*args, **kwargs)
        except (paramiko.SSHException, paramiko.SFTPError, socket.error,
                EOFError, xmlrpclib.Error) as ex:
            msg = str(ex)
            print(msg)
            try:
                raise ConnectionError(msg)
            except TypeError:
                raise SubConnectionError(msg)
    return wrapper_function


__all__ = [
    ApiAuthError,
    ApiConflictError,
    ApiConnectionError,
    ApiError,
    ApiInternalError,
    ApiInvalidRequestError,
    ApiNotFoundError,
    ApiTimeoutError,
    ApiUnavailableError,
    ApiValidationFailedError,
    SdkEndpointNotFound,
    SdkEntityNotFound,
    SdkError,
    SdkTypeNotFound,
    ApiResponseError,
    ApiNotSupportedError,
    BlockerError,
    CliError,
    ConnectionError,
    FeatureNotSupportedError,
    PollingException,
    EquipmentNotFoundError,
    DateraGuiError,
    GuiResponseError,
    GuiElementNotFoundError,
    GuiElementNotSelectableError,
    GuiElementNotVisibleError,
    GuiElementNotClickable,
    GuiElementDetachedFromDOMError,
    GuiElementAttributeNotFoundError,
    GuiAlertNotFoundError,
    TimeoutError,
    InvalidElementLocator,
    with_exception_translation]
