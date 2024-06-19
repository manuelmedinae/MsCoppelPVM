import os
import socket
import time
import datetime
from fluent import asyncsender as sender


class LoggerWrapper(object):
    """
        Clase para mantener el mismo formato de los logs,
        para los mensajes fluentd
    """

    name = 'FLUENT_LOGS'

    __name = None
    __APP = ''
    __SERVICE = ''
    __VERSION = ''
    __BROKERS = ''
    __FRAMEWORK_VERSION = ''
    __HOSTNAME = ''
    __PRODUCTION = False

    __LEVELS = {
        'FATAL': 60,
        'ERROR': 50,
        'WARNING': 40,
        'INFO': 30,
        'DEBUG': 20,
        'TRACE': 10,
    }

    __logger = None

    def __init__(self, name):
        self.__name = name
        # Conexion con Fluentd
        self.__logger = sender.FluentSender(
            'microservices.logs',
            host=os.environ.get('FLUENTD_HOST', 'td-agent'),
            port=os.environ.get('FLUENTD_PORT', 24224),
            queue_circular=True
        )
        # Asignar el hostname
        self.__HOSTNAME = socket.gethostname()
        # Production
        self.__PRODUCTION = False if os.environ.get(
            'PRODUCTION', None) is None else True

    def set_conf(self, app, service, version, brokers, frmversion):
        """
            Metodo para realizar la configuraciones extra del servicio
            para su procesamiento en Fluentd.
        """
        self.__APP = app
        self.__SERVICE = service
        self.__VERSION = version
        self.__BROKERS = brokers
        self.__FRAMEWORK_VERSION = frmversion

    def trace(self, message, data={}):
        self._log('TRACE', message, data)

    def debug(self, message, data={}):
        # Validar si esta modo productivo.
        if os.environ.get('PRODUCTION', False):
            self._log('DEBUG', message, data)

    def info(self, message, data={}):
        self._log('INFO', message, data)

    def warn(self, message, data={}):
        self._log('WARNING', message, data)

    def warning(self, message, data={}):
        return self.warn(message, data)

    def fatal(self, message, data={}):
        self._log('FATAL', message, data)

    def error(self, message, error=None, data={}):
        self._log('ERROR', message, data, error)

    def log(self, level, message, data={}):
        self._log(level, message, data)

    def _log(self, level, message='', data={}, error=None):
        # Recuperar el level
        # _level = self.__LEVELS.get(level, 10)
        # Formato
        logs_dict = {
            "schema-version": "1.0",
            'level': level,
            "error-origin": "INTERNAL",
            "tracing_id":    None,
            "dockerID":      None,
            'hostname': self.__HOSTNAME,
            "microservice":  "{}_{}_{}".format(
                self.__APP, self.__VERSION, self.__SERVICE
            ),
            "message": message,
            "operation": None,
            "object": data,
            "stacktrace": None,
            'timestamp': datetime.datetime.now().isoformat(),
        }

        # Ver si envio un error
        if error is not None:
            logs_dict.update({
                "stacktrace": error,
            })

        # Logss
        """ log = {
            '@timestanp': datetime.datetime.now().isoformat(),
            'log_type': 'app',
            'level': _level,
            'type': level,
            'production': self.__PRODUCTION,
            'app': self.__APP,
            'service': self.__SERVICE,
            'version': self.__VERSION,
            'brokers': self.__BROKERS,
            'framework_version': self.__FRAMEWORK_VERSION,
            'hostname': self.__HOSTNAME,
            'message': message,
            'data': data,
        } """

        # Publicar el log
        self.__logger.emit_with_time(self.__APP, int(time.time()), logs_dict)
