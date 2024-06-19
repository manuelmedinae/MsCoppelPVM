from .ErrorMs import ErrorMs


class BadRequest(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 400, headers)


class NotAuthenticated(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 401, headers)


class PaymentError(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 402, headers)


class Forbidden(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 403, headers)


class NotFound(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 404, headers)


class MethodNotAllowed(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 405, headers)


class NotAcceptable(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 406, headers)


class Timeout(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 408, headers)


class Conflict(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 409, headers)


class LengthRequired(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 411, headers)


class Unprocessable(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 422, headers)


class TooManyRequests(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 429, headers)


class GeneralError(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 500, headers)


class NotImplemented(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 501, headers)


class BadGateway(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 502, headers)


class Unavailable(ErrorMs):
    def __init__(self, code, headers={}, message=""):
        super().__init__(code, message, 503, headers)
