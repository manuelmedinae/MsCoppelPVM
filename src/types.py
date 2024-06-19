from enum import Enum, unique


@unique
class Types(Enum):
    """
        Tipos de elementos soportados en los microservicios
    """
    WORKER = 1
    FORK = 2


@unique
class TypesActions(Enum):
    CREATE = 1
    GET = 2
    DELETE = 3
    LIST = 4
    LISTENER = 5
    UPDATE = 6
    ERRORS = 7
    FORKS = 8
    LIVENESS = 9
    READNESS = 10


@unique
class Actions(Enum):
    """
        Acciones que se ejecutan desde un servicio
    """
    CREATE = 1
    GET = 2
    DELETE = 3
    LIST = 4
    LISTENER = 5
    UPDATE = 6
    LIVENESS = 7
    READNESS = 8

class HttpError(Enum):
    BadRequest = 400
    NotAuthenticated = 401
    PaymentError = 402
    Forbidden = 403
    NotFound = 404
    MethodNotAllowed = 405
    NotAcceptable = 406
    Timeout = 408
    Conflict = 409
    LengthRequired = 411
    Unprocessable = 422
    TooManyRequests = 429
    GeneralError = 500
    NotImplemented = 501
    BadGateway = 502
    Unavailable = 503
