# -*- coding: utf-8 -*-
'''
Provides the Credentials classes

This package depends on the siteconfig package.
'''
__copyright__ = "Copyright 2020, Datera, Inc."


class Credentials(object):
    '''
    This object stores authentication information for accessing equipment
    Typically, and for now, this is just a username and password
    '''
    def get_username(self):
        '''
        Gets the login name, which could be None if not applicable for
        this authentication type.
        '''
        raise TypeError("Not supported for this credentials type")

    def get_password(self):
        '''
        Gets the password, which could be None if not applicable for
        this authentication type.
        '''
        raise TypeError("Not supported for this credentials type")


class _CredentialsUserPass(Credentials):
    '''
    This object stores authentication information for accessing equipment
    based on a username and password.
    '''

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_username(self):
        return self.username

    def get_password(self):
        return self.password


def from_user_pass(username, password):
    '''
    Returns a Credentials object
    Parameters:
      username (str)
      password (str)
    '''
    if username is None:
        raise ValueError("username must not be None")
    if password is None:
        raise ValueError("password must not be None")
    return _CredentialsUserPass(username, password)
