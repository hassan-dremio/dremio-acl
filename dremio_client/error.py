# -*- coding: utf-8 -*-

class DremioException(Exception):
    """
    base dremio exception
    """

    def __init__(self, msg, original_exception, response=None):
        super(DremioException, self).__init__(msg + (": %s" % original_exception))
        self.original_exception = original_exception
        self.response = response


class DremioUnauthorizedException(DremioException):
    pass


class DremioPermissionException(DremioException):
    pass


class DremioNotFoundException(DremioException):
    pass


class DremioBadRequestException(DremioException):
    pass
