import os
import socket
from bottle import Bottle


class HttpServer:
    """
        Clase para el manejo de peticiones utilizando un servior HTTP en lugar
        de las colas de mensajes.
    """

    def __init__(self, topic, app_name, version, service_name):
        # Variables de configuracion
        self._routes = []

        # Configuracion del servidor
        self._host = '0.0.0.0'
        self._port = os.environ.get('HTTP_PORT', 8000)

        # Configuracion del servicio
        self.__hostname = socket.gethostname()
        self._topic = topic
        self._app_name = app_name
        self._version = version
        self._service_name = service_name

        # Configuracion del bottle
        self._app = Bottle()
        self.__init_list_routes()  # Generar las URL
        self.__init_router()  # Iniciar las rutas del servidor

    def __init_list_routes(self):
        """
            Metodo para iniciar la lista de las rutas
        """
        self._routes = [
            "/{}/api/{}/{}".format(self._app_name,
                                   self._version, self._service_name),
            "/{}/api/{}/{}/<id>".format(self._app_name,
                                        self._version, self._service_name),
            "/api/{}/{}".format(self._version, self._service_name),
            "/api/{}/{}/healthcheck/liveness".format(self._version, self._service_name),
            "/api/{}/{}/healthcheck/readness".format(self._version, self._service_name),
            "/api/{}/{}/<id>".format(self._version, self._service_name)
        ]

    def __init_router(self):
        """
            Metodo para realizar la inicializacion de las rutas
            para el servidor.
        """
        for route in self._routes:
            self._app.route(
                route,
                ['GET', 'POST', 'UPDATE', 'DELETE', 'OPTION'],
                self._process_handler
            )

    def _process_handler(self, id=None):
        """
            Handler del servicio http
        """
        pass

    def start(self):
        """
            Metodo para iniciar el servidor HTTP
        """
        self._app.run(host=self._host, port=self._port)
