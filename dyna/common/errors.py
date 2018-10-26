class DynaError(Exception):
    """Base class for exceptions in this module."""
    pass


class GRPCError(DynaError):
    def __init__(self, code, reason):
        super().__init__('GRPC Error: {0} - {1}'.format(code, reason))
        self.code = code
        self.reason = reason


class ServiceError(DynaError):
    def __init__(self, service, method):
        super().__init__('Error in {0}.{1}'.format(service, method))
        self.service = service
        self.method = method


class DynaValueError(DynaError):
    def __init__(self, value, expected_type):
        super().__init__('Value Error: {0} not of type: {1}'.format(value, expected_type))
        self.value = value
        self.expected_type = expected_type



class DatatypeError(DynaError):
    """Exception raise for invalid datatypes."""
    def __init__(self, value):
        self.message = "Invalid datatype specified: {0}".format(value)


class LoaderError(DynaError):
    """Exception raised when a loader encounters an error"""
    def __init__(self, loader, message="Loader Error"):
        self.loader = loader
        self.message = '{0}: Type: {1}'.format(message, loader.__name__)


