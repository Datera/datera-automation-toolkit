"""
Provides the TestBedSetup base class
"""


class BaseTestBedSetup(object):
    """
    Base class for TestBedSetup objects
    Defines the interface
    Concrete implementations will over-ride most of this
    """
    app_instances = []
    storage_instances = []
    volumes = []
    clients = []

    @staticmethod
    def configure():
        """ A NOOP for this object """
        pass

    @staticmethod
    def cleanup():
        """ A NOOP for this object """
        pass

    @staticmethod
    def get_client_and_si_mapping():
        """ Not applicable for this object, so returns {} """
        return {}
