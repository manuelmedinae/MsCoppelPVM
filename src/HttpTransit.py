import os
import socket
import uuid
import copy
import json
import time
import _thread
from waitress import serve
from bottle import Bottle, request, HTTPResponse
from .Util import getItemBy
from .loggs import Loggs
from .LogssBottle import log_to_logger
from .version_framework import version as version_f
import functools


class HttpTransit:
    """
        Clase para el manejo de peticiones utilizando un servior HTTP en lugar
        de las colas de mensajes.
    """

    # Logs de la aplicacion
    logs = Loggs('Service')

    __REQUEST = {}

    # BaseClass.__init__(self, topico, opt.Hosts, topico)
    def __init__(self, topic, app_name, version, service_name):

        # Variables de configuracion
        self._routes = []
        self.currentRoute = ""

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
        self._app.install(log_to_logger)  # Sustituir el formato de logs
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
            "/{}/api/{}/{}/healthcheck/liveness".format(self._app_name,self._version, self._service_name),
            "/{}/api/{}/{}/healthcheck/readness".format(self._app_name,self._version, self._service_name),
            "/api/{}/{}/<id>".format(self._version, self._service_name)
        ]

    def __init_router(self):
        """
            Metodo para realizar la inicializacion de las rutas
            para el servidor.
        """
        for route in self._routes:
            self.currentRoute = route
            self._app.route(
                route,
                ['GET', 'POST', 'UPDATE', 'DELETE', 'OPTION', 'PUT'],

                functools.partial(self._process_handler, param=route)
            )

    def _process_handler(self, id=None, param = None):
        """
            Handler del servicio http
        """
        # Tiempo de ejecucion
        start_time = time.time()

        # Resp
        RESP = {}

        # Generar la metadata y el Key de la operacion
        Key_op, meta = self.__create_meta(id, param)

        # Lanzar el metodo
        _thread.start_new_thread(self._message, (meta,))

        # Error
        IS_ERROR = False

        # Codigo de error
        CODE = 200

        try:
            # Esperar la respuesta
            RESP = self.coro_manager(self.get_response_operation(Key_op))
        except Exception as e:
            self.logs.error(e)
            IS_ERROR = True
            # Crear el elemento de respuesta de error.
            RESP = {
                "meta": {
                    "status": 'ERROR',
                    "id_transaction": Key_op,
                },
                "data": {
                    "response": {
                        "errorCode": -9,
                        "userMessage": '-35',
                    },
                }
            }

        # Meta
        META = RESP.get('metadata', {})

        # Recuperar la respuesta del response
        RESPONSE = RESP.get('response', {})

        # Actualizar el tiempo de operacion
        if not RESPONSE.get('meta', None) is None:
            RESPONSE['meta'].update({
                "time_elapsed": time.time() - start_time,
            })

        # Validar si es error controlado
        RC = getItemBy('data.response.errorCode', RESPONSE)

        # Validar si es el error
        if not IS_ERROR:
            if RC is not None:
                IS_ERROR = True

        # Codigo
        if IS_ERROR:
            CODE = 500

        # Recuperar el http data
        HTTP = META.get('http', {
            "code": CODE,
            "message": "",
            "headers": []
        })

        return HTTPResponse(status=HTTP.get('code'), body=RESPONSE)

    async def get_data(self, id_op):
        """
        """

        RESP = None

        try:
            # Solicitar la respuesta de la operacion
            RESP = await self.get_response_operation(id_op)
        except Exception as e:
            self.logs.error(e)

        return RESP

    def coro_manager(self, coro):
        """
            Metodo para poder hacer el llamado de la funcion Async
            sin necesidad de usar await.

            @params coro Metodo async a llamar
        """
        try:
            coro.send(None)
        except StopIteration as stop:
            return stop.value
        except Exception as e:
            raise e

    async def get_response_operation(self, id_operacion):
        """
            Metodo para recuperar los datos de una operacion
            que es finalizada, o en su caso, eliminar la espera,
            si le tomo el tiempo de gracia
        """

        RESP_MICRO = None

        while self.__REQUEST.get(id_operacion) is None:
            # print(self.__REQUEST)
            # Comparar el tiempo transcurrido
            # await asyncio.sleep(.100)
            pass

        # Almacenar la respuesta
        RESP_MICRO = copy.deepcopy(self.__REQUEST.get(id_operacion, {}))

        # Eliminar el elemento
        del self.__REQUEST[id_operacion]

        # Retornar el resultado de la operacion
        return RESP_MICRO

    def _message(self, msg):
        """
            Metodo para el procesamiento de mensajes de
            Nats.
        """
        pass

    def __create_meta(self, id_resource=None, param = None):
        """
            Metodo para la generacion de la metadata
        """
        # Generar el id de la operacion
        KEY = self.__generate_key()

        # Agregar la operacion
        self.__REQUEST.update({KEY: None})

        return KEY, {
            "uuid": None,
            "metadata": {
                "ruta": param,
                "id_operacion": KEY,
                "id_transaction": self.__generate_key(),
                "intents": 1,
                "callback": self._topic,
                "owner": self._topic,
                "uowner": self._topic,
                "uworker": "{}_{}".format(
                    self._topic,
                    KEY,
                ),
                "worker": self._topic,
                "asynchronous": False,
                "mtype": "input",
                "bifurcacion": False,
                "time": None,
                "from": self.__hostname,
                "method": request.method,
                "uuid": id_resource,
                "callbacks": [{"method": request.method, "name": self._topic}],
                "deviceType": "SERVER",
                "hostname": self.__hostname,
                "framework": "Python_{}".format(version_f),
                "filters": dict(request.query.decode()),
                "span": None,
                "http": None
            },
            "headers": dict(request.headers),
            # json.load(request.body if request.body else {}),
            "data": json.loads(request.body.read().decode("utf-8")) if request.body.read() else {},
            "response": None
        }

    def _send(self, reply, msj, idTransaction=""):
        """
            Metodo para lanzar la respuesta
        """
        if not msj.get('metadata') is None:
            if not msj['metadata'].get('id_operacion') is None:
                self.__REQUEST.update(
                    {msj['metadata'].get('id_operacion'): msj})

    def __generate_key(self, range_key=32):
        """
            Metodo para generar claves
        """
        k = str(uuid.uuid4())
        return (k)

    def start(self):
        """
            Metodo para iniciar el servidor HTTP
        """
        # DEBUG = True

        # Validar si es produccion
        if os.environ.get('PRODUCTION', None):
            # DEBUG = False
            pass

        self.logs.info('REST ENABLED: True')

        serve(self._app, host=self._host, port=self._port)

        # Iniciar el servidor
        # self._app.run(host=self._host, port=self._port,
        #              quiet=True, debug=DEBUG)
