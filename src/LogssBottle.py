from functools import wraps
from bottle import request, response
from .loggs import Loggs
import os

# Logs de la aplicacion
logs = Loggs('Service')


def log_to_logger(fn):
    @wraps(fn)
    def _log_to_logger(*args, **kwargs):
        actual_response = fn(*args, **kwargs)

        # Los logs solo se muestra si no es productivo
        if os.environ.get('PRODUCTION', None) is None:
            logs.info("{} {} {} {}".format(request.remote_addr,
                                           request.method,
                                           request.url,
                                           response.status))
        return actual_response
    return _log_to_logger
