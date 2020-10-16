
class DagrException(Exception):
    def __init__(self, value):
        super(DagrException, self).__init__(value)
        self.parameter = value

    def __str__(self):
        return str(self.parameter)


class DagrPremiumUnavailable(DagrException):
    def __init__(self):
        super().__init__(
            'Premium content unavailable')

class DagrHTTPException(DagrException):
    pass

class Dagr404Exception(DagrHTTPException):
    def __init__(self):
        super().__init__(
            'HTTP 404 error')
    @property
    def httpcode(self):
        return 404


class Dagr403Exception(DagrHTTPException):
    def __init__(self):
        super().__init__(
            'HTTP 403 error')
    @property
    def httpcode(self):
        return 403
