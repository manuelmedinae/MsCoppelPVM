import sys
import inspect
import json
import os
import socket
import copy
import base64
import datetime
import traceback
import uuid
import re
import _thread
import asyncio
from jaeger_client import Config
from opentracing import Format, child_of
from opentracing.ext import tags as ext_tags
from abc import abstractmethod, ABC
from .options import Options
from .Worker import Worker
from .Fork import Fork
from .ForkHttp import ForkHttp
from .ErrorMs import ErrorMs
import time
from .Util import validForksConf, getItemBy
from .version_framework import version
from http.server import BaseHTTPRequestHandler, HTTPServer
from .types import TypesActions
from .HttpResponse import HttpResponse
import redis
import hashlib
from contextvars import ContextVar, copy_context
import os.path

if os.environ.get('KAFKA', None):
    from .ms_base import KafkaBase as BaseClass
elif os.environ.get('NATS', None):
    from .NatsBase import NatsBase as BaseClass
elif os.environ.get('REST_ENABLED', None):
    from .HttpTransit import HttpTransit as BaseClass
else:
    # En caso que se no indique por variable de entorno
    from .ms_base import KafkaBase as BaseClass

# Checar si importar o no la librera de dynatrace
if os.environ.get('AUTODYNATRACE', None):
    import autodynatrace

class Microservices(BaseClass, ABC):
    """
        Clase con la logica necesaria para utilizar sobre los
        microservicios, se debe implementar los metodos segun
        como se requiera.
    """
    # Datos de configuracion
    __OPT = {}
    # Acciones del microservicio
    __ACTIONS = {}
    # Lista de errores definidos por el usuario
    __ERRORS = {
        '-97': 'No existe la accion que esta tratando de llamar',
        '-98': 'Ocurrio una error, favor de intentar de nuevo.',
        '-99': 'Ocurrio una excepcion, favor de intentar de nuevo.',
        '0': 'Smoke Test',
        '-1': 'Este servicio no puede ser consumido de forma externa'
    }
    # Logica de la aplicacion
    __APP = None

    # Topico
    __TOPIC__ = ''

    # Hostname
    __HOSTNAME = ''

    # Fork Configurados en la clase
    __FORKS = None

    # Indicar si es el servicio es una bifurcacion
    ___isForks = False

    # OpenTracing Jaeger
    ___Tracer = None

    # Indicar si se usara los logs de fluentd
    __fluentd_logs = False

    """
        Clase que contiene la logica del microservicio.
    """

    # Instancia de redis
    redisclient = None

    # Contex Span de tracer
    __SPAM = ContextVar('span')

    def __init__(self, opt):
        # Validar que se pase un objeto de configuracion correcto
        if not isinstance(opt, Options):
            self.logs.error('No se proporciono una configuracion correcta')
            sys.exit(-1)

        # Asignar los datos de la configuracion para su acceso
        self.__OPT = opt

        # Construccion del topico
        if opt.App == 'appcoppel' and opt.Version == 'v1':
            topico = opt.Name
        else:
            topico = "{}_{}_{}".format(opt.App, opt.Version, opt.Name)

        # Asignar el topico
        self.__TOPIC__ = topico

        # Recuperar el hostname del servidor
        self.__HOSTNAME = socket.gethostname()

        # Inicializacion del tracer
        self.initilizeTracer()

        # Inicializacion de HealthCheck
        _thread.start_new_thread(
            self.startHttpHealthCheck, (self.HealthCheck, self.smoketest))

        # Buscar las acciones
        self.__initActions()

        # Validar si es Productivo o si se activaron los logs a fluent
        if not os.environ.get('LOGS_FLUENT', None) is None:
            self.__fluentd_logs = True
            # Enviar la configuracion necesaria
            self.logs.set_conf(opt.App, opt.Name,
                               opt.Version, opt.Hosts, version)

        # Crear el administrador de la logica de la aplicacion.
        if self.__FORKS is None:
            self.__APP = Worker(self.__TOPIC__)
        else:
            self.__fork_init()

        # Inicializar el redis
        self.initRedis()

        # llamar el constructor padre
        # (self, topic, app_name, version, service_name):
        if os.environ.get('REST_ENABLED', None):
            BaseClass.__init__(self, topico, opt.App, opt.Version, opt.Name)
        else:
            BaseClass.__init__(self, topico, opt.Hosts, topico)

        if opt.isNats:
            # iniciar la aplicacion
            """
                NOTA:
                    Ninguna instruccion debajo del etodo run_forever()
                    se ejecutara.
            """
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(asyncio.wait([self.run()]))
            # self.loop.run_until_complete(self.run())
            try:
                self.loop.run_forever()
            finally:
                self.loop.close()

        if os.environ.get('REST_ENABLED', None):
            self.__loop_async(self.start())

    def __fork_init(self):
        self.logs.info(
            'Se Inicia el servicio con soporte para bifurcaciones')

        if not os.environ.get('REST_ENABLED', None) is None:
            if os.environ.get('URL_ENDPOINT_SERVICES', None) is None:
                self.logs.error(
                    "No se proporciono el valor para URL_ENDPOINT_SERVICES"
                )
                sys.exit(-1)
            else:
                self.logs.info("Se inicia el soporte de Fork Http")
                self.__APP = ForkHttp(self.__TOPIC__)
        else:
            self.__APP = Fork(self.__TOPIC__)

        self.___isForks = True

    def __loop_async(self, fn):
        """
            Metodo para mandar llamar el ciclo del async
        """
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.wait([fn()]))
        # self.loop.run_until_complete(self.run())
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def initRedis(self):
        """
            Metodo para inicializar el redis
        """
        if not os.environ.get('REDISVALIDATE', None) is None:
            self.logs.info("Redis {}".format(
                os.environ.get('REDISVALIDATE', None)))

        if os.environ.get('REDISVALIDATE', None) is None:
            self.logs.warn('No se encuentra activado la validacion por redis')
        else:
            port = os.environ.get('REDIS_PORT', 6379)

            __pool_redis = redis.ConnectionPool(
                host=os.environ.get("REDISVALIDATE"), port=6379, db=0)
            self.redisclient = redis.Redis(connection_pool=__pool_redis)

            if self.redisclient is None:
                self.logs.warn(
                    'No fue posible conectarse al servidor \
                        de redis en la direccion: {}:{}'.format(
                        os.environ.get('REDISVALIDATE'), int(port)
                    ))
                sys.exit(-1)
            self.logs.info('Se encuentra activado la validacion por redis')

    def hasProcessMessage(self, data):
        """
            Metodo para validar que una operacion ya fue relizada.
        """
        if os.environ.get('REDISVALIDATE', None) is None:
            return False
        else:
            shahex = hashlib.sha224(bytes(data, 'utf-8')).hexdigest()
            response_redis = self.redisclient.getset(shahex, data)
            self.redisclient.expire(shahex, (10 * 60)) # Siempre asignar el tiempo
            if response_redis is None:
                return False
            return True

    def initilizeTracer(self):
        j = 'JAEGER_LOGS'
        config = Config(
            config={
                'sampler': {
                    'type': 'const',
                    'param': 1,
                },
                'local_agent': {
                    'reporting_host': os.environ.get(
                        'JAEGER_AGENT_HOST',
                        'jaeger-agent.logging',
                    ),
                    'reporting_port': os.environ.get(
                        'JAEGER_AGENT_PORT',
                        6831,
                    ),
                },
                'logging': False if os.environ.get(j, None) is None else True,
                'tags': {
                    '@coppel-py-version': version
                }
            },
            service_name=self.__TOPIC__,
            validate=True,
        )
        self.___Tracer = config.initialize_tracer()

    def send(self, topic, msj, idTransaction=""):
        """
            Metodo para el envio de datos a Nats

            @params reply Elemento al que se le responde
            @params msj Mensaje que enviara
            @params idTransaction Id de la transaccion
        """
        self._send(topic, msj, idTransaction)

    def HealthCheck(self):
        return self.validateConnection()

    def startHttpHealthCheck(self, healthcheck, smoketest):
        if os.environ.get('PRODUCTION', False):
            class myHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    message_parts = []
                    code = 0
                    try:
                        if healthcheck() and smoketest():
                            code = 200
                            message_parts.append("OK")
                    except Exception:
                        code = 500

                    if code < 100:
                        code = 500
                        message_parts.append("ERROR")

                    message = '\r\n'.join(message_parts)
                    self.send_response(code)
                    self.send_header(
                        'Content-Type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(message.encode('utf-8'))

                def log_message(self, format, *args):
                    return

            self.logs.info('[HealthCheck] listening on 0.0.0.0:80')

            server = HTTPServer(('', 80), myHandler)
            server.serve_forever()
        else:
            self.logs.warning('HttpHealthCheck is Disabled')

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

    def _send_error(self, data, id_transaction):
        data_error = {
            "id": id_transaction,
            "_id": id_transaction,
            "error": data,
            "servicio": self.__TOPIC__,
            "fecha": int(time.time() * 1000)
        }

        if isinstance(data, dict):
            if "error" in data and "_id" in data:
                data.update(
                    {"fecha": int(time.time() * 1000)})
                data_error = data

        self._send(
            os.environ.get('TOPIC_ERROR_CRITIC', 'Errores_criticos'),
            data_error,
            id_transaction
        )

    def json_to_b64(self, json_in):
        """
            Metodo para convertir un diccionaro al formato
            correcto de los mensajes.

            @param json_in Dict de entrada
        """
        return base64.b64encode(str.encode(json.dumps(json_in)))

    def without_response(self, topic, data={}, id_transaction=None, action=None):
        """
            Metodo para el envio de una peticion a un topico, sin esperar
            una respuesta del mismo.
        """
        ctx_op = self.__SPAM.get(
            {'span': None, "id_transaction": uuid.uuid4()})

        span_ctx = ctx_op.get('span')

        carrier = {}

        if topic == "Errores_criticos":
            # self._send(topic, data, id_transaction)
            self._send_error(data, id_transaction)
        else:

            id_trans = id_transaction if id_transaction is not None else ctx_op.get(
                "id_transaction")

            if span_ctx is not None:
                self.___Tracer.inject(
                    span_ctx, format=Format.TEXT_MAP, carrier=carrier)

            DATA_SEND = {
                "metadata": {
                    "uuid": None,
                    "id_operacion": str(uuid.uuid4()),
                    "id_transaction": id_trans,
                    "intents": 1,
                    "callback": None,
                    "callbacks": [],
                    "owner": self.__TOPIC__,
                    "uowner": self.__TOPIC__,
                    "uworker": "{}_WITHOUT_RESPONSE".format(self.__TOPIC__),
                    "worker": self.__TOPIC__,
                    "asynchronous": False,
                    "mtype": "WITHOUT_RESPONSE",
                    "bifurcacion": False,
                    "time": int(time.time() * 1000),
                    "from": socket.gethostname(),
                    "method": "GET" if action is None else action,
                    "deviceType": "",
                    "span": carrier if span_ctx is not None else None,
                    "filters": None,
                },
                "data": data,
                "headers": {
                    "Asynchronous": False,
                    "Authorizacion": ""
                }
            }

            self._send(topic, DATA_SEND, id_trans)

    def _message(self, msg):
        """
            Metodo que se encagra de procesar todos los
            mensajes que llegas desde Kafka.
        """
        # metodo que se ejecutara
        mth = None

        # Recuperar los datos de otro modo regresar
        data = msg.get('data', {})

        # Recuperar la metadata
        meta = msg.get('metadata', {})

        # Configuracion del worker
        confForks = None

        # Parametros solicitados
        paramsConf = []

        # Contexto de la operacion
        ctx = None

        # Span
        span = None

        # Recuperar el rootSpan
        # msg['metadata']['span'] if 'span' in msg['metadata'] else None
        rootSpan = None

        if not msg.get('metadata', None) is None:
            m = msg.get('metadata')
            if not m.get('span', None) is None:
                rootSpan = m.get('span')

        # Validar si es un smoktest
        if 'smoketest' in list(data):
            span = None

            try:
                if rootSpan is None:
                    span = self.___Tracer.start_span(
                        operation_name='smoketest')
                else:
                    span = self.___Tracer.start_span(
                        operation_name='smoketest', child_of=rootSpan)
            except Exception as e:
                self.logs.warning(e)
                span = self.___Tracer.start_span(operation_name='smoketest')

            # Tag
            span.set_tag(ext_tags.HTTP_METHOD, msg['metadata']['method'])

            # Ejecutar la funcion del smoketest
            if self.smoketest():
                # To do esta bien
                self.__response(msg, 0, True, msg.get('uuid', None), span=span)
            else:
                # Algo salio mal
                self.__response(
                    msg, -1, True, msg.get('uuid', None), span=span)

            span.finish()
            return

        # Validacion de redis
        if self.hasProcessMessage('{}_{}_{}'.format(
            self.__TOPIC__,
            meta.get('id_transaction', ''),
            json.dumps(
                msg.get('data', {})
            )
        )):
            span = None

            spr = None

            if rootSpan is None:
                span = self.___Tracer.start_span(
                    operation_name='replication')
            else:
                if isinstance(rootSpan, dict):
                    spr = self.___Tracer.extract(Format.TEXT_MAP, rootSpan)

                span = self.___Tracer.start_span(
                    operation_name='replication',
                    references=child_of(spr if spr is not None else rootSpan)
                )

            if os.environ.get('DISABLED_LOGS_VALIDATE', None) is None:
                self.logs.error('Se recibio una operacion que ya se realizo')

            span.set_tag(ext_tags.ERROR, 'hasProcessMessage')

            span.finish()
            return True

        # Solo actualizar los datos si e el servicio original
        if self.__TOPIC__ == msg['metadata']['callback']:
            if not msg['metadata'].get('data_inicial', None) is None:
                # Recuperar el metodo original
                if not msg['metadata'].get('original_method', None) is None:
                    msg['metadata']['method'] = msg['metadata']['original_method']

                # Recuperar el metodo original
                if not msg['metadata'].get('data_inicial', None) is None:
                    msg['data'] = msg['metadata']['data_inicial']

        # Validar si se encuentra en modo Debug
        if self.__OPT.Debug and self.__fluentd_logs:
            _data_ = {}
            _data_.update({'owner': meta.get('owner')})
            _data_.update({'method': meta.get('method')})
            _data_.update({'headers': msg.get('headers', {})})
            _data_.update({'data': msg.get('data', {})})
            self.logs.info('ENTRADA {}'.format(json.dumps(_data_)))
        elif self.__OPT.Debug:
            self.logs.info(
                "\n ENTRADA: {}\n METODO: {}\n HEADERS: {}\n DATA: {}".format(
                    meta.get('owner'),
                    meta.get('method'),
                    json.dumps(msg.get('headers', {})),
                    msg.get('data', {})
                ))

        # Procesar el mensaje
        try:
            mth = self.__getMethod(msg.get('metadata'))
        except Exception as identifier:
            # No fue posible recuperar una accion para el evento
            self.logs.error(
                'No fue posible recuperar una accion para el evento {}'.format(
                    identifier
                )
            )

        # Validar que exista
        try:
            if hasattr(mth, '__CONF_FORKS__'):
                methodConfFork = getattr(mth, '__CONF_FORKS__')
                # Recuperar la configuracion
                confForks = self.__getConfServiceFork(methodConfFork)
        except Exception as e:
            self.logs.error(
                'Ocurrio un error al recuperar la configuracion del servicio: {}'.format(e))

        # Buscar los parametros
        try:
            if hasattr(mth, '__CONF_PARAMS__'):
                paramsConf = getattr(mth, '__CONF_PARAMS__')
        except Exception as e:
            self.logs.error(e)

        # Validar si es un string o un diccionario
        if rootSpan is not None:
            rootSpan = self.___Tracer.extract(Format.TEXT_MAP, rootSpan)

        if mth is None:
            span = self.___Tracer.start_span(operation_name='No identificado')
        else:
            span = self.___Tracer.start_span(operation_name=getattr(
                mth, '__FUNC_NAME__'), references=child_of(rootSpan))

        span.set_tag(ext_tags.HTTP_METHOD, msg['metadata']['method'])

        RESP = {}

        # Enviar el mensaje al que procesa [Worker, Fork]
        try:
            # Configuracion de los forks
            if confForks is None:
                self.__APP = Worker(self.__TOPIC__)
                # Copiar el contexto de la ejucion
                ctx = copy_context()
            else:
                self.__fork_init()
                # self.__APP = Fork(self.__TOPIC__)
                self.__APP.confForks(confForks)
                # Copiar el contexto de la ejucion
                ctx = copy_context()

            # Indicar si es una bifurcacion
            msg['metadata']['bifurcacion'] = True if confForks else False

            # Almacenar el valor de span en el contexto
            self.__SPAM.set({
                "span": span,
                "id_transaction": meta.get('id_transaction', uuid.uuid4())
            })

            # ejecutar el proceso
            RESP = ctx.run(self.__APP.process, span, msg, mth, paramsConf)

            # Ruta [SOLO NATST]
            id_route_nats = getItemBy('response.uuid', RESP)

            if not msg['metadata']['mtype'] == 'WITHOUT_RESPONSE':
                RESPONSE = RESP.get('response', {})
                ERROR_CODE = RESP.get('errorCode')
                ERROR_WORKER = RESP.get('errorWorker', None)
                # Enviar la respuesta
                self.__response(
                    RESPONSE,
                    ERROR_CODE,
                    span=span,
                    nats_route=id_route_nats,
                    errorResp=ERROR_WORKER,
                )

        except ErrorMs as error:
            span.set_tag(ext_tags.ERROR, True)

            # Recuperar el registro del error
            error_reg = self.___getErrorId(error.errorCode)

            # Validar si es un diccionario
            isDict = isinstance(error_reg, dict)

            span.log_kv({
                'event': 'error',
                'error.code': error.errorCode,
                'error.message': error_reg.get(
                    'message', ""
                ) if isDict else error_reg,
                'error.stack': traceback.format_exc()
            })

            self.logs.error(error_reg.get(
                'message', ""
            ) if isDict else error_reg)

            # Verificar si es un codigo de error ya especificado
            if error.http_code is not None:
                msg["response"]["data"]["response"] = HttpResponse(
                    msg["response"]["data"]["response"],
                    error.http_code,
                    error.message,
                    error.headers
                )
            else:
                if isDict and "http_code" in error_reg:
                    msg["response"]["data"]["response"] = HttpResponse(
                        msg["response"]["data"]["response"],
                        error_reg["http_code"],
                        error_reg.get('message', ""),
                        error.headers
                    )

            # Validar si es async
            if not msg['metadata']['mtype'] == 'WITHOUT_RESPONSE':
                self.__response(
                    msg, error.errorCode, span=span, nats_route=msg.get(
                        'uuid', None), errorResp=RESP.get('errorWorker', None),
                )  # Enviar el error

        except Exception as e:
            span.set_tag(ext_tags.ERROR, True)
            span.log_kv({'event': 'error', 'error.message': str(
                e), 'error.stack': traceback.format_exc()})
            self.logs.error(e)
            # Enviar el error
            if not msg['metadata']['mtype'] == 'WITHOUT_RESPONSE':
                self.__response(
                    msg, -99, span=span, nats_route=msg.get('uuid', None),
                    errorResp=RESP.get('errorWorker', None),
                )

        span.finish()

    def __response(self, data, errorCode, isSmokeTest=False,
                   nats_route=None, span=None, errorResp=None):
        """
            Metodo para enviar la respuesta a kafka
        """

        # Diccionario de la respuesta
        Resp = {}

        # Datos http.
        HTTP = None

        # Respuesta del servicio
        RESPONSE_DATA = None if data.get('response', None) is None else \
            data["response"]["data"]["response"]

        # Validar si data no es de tipo httpResponse
        if (isinstance(RESPONSE_DATA, HttpResponse)):
            http_data = data["response"]["data"]["response"]

            # Validar si es None
            if http_data.code is None:
                # Registro del error
                reg_error = self.___getErrorId(errorCode)
                # Validar si existe
                if 'http_code' in reg_error:
                    # Retornar el codigo de error correspondiente
                    http_data.code = reg_error.get('http_code', 500)

            HTTP = {
                "code": http_data.code,
                "headers": http_data.headers,
                "message": http_data.message
            }
            # Reset de data
            data["response"]["data"]["response"] = http_data.data

        # Crear una copia de la respuesta
        dataResponse = data

        # Topico de respuesta
        TOPIC_RESP = None

        # Version del topico de respuesta
        VERSION_RESP = None

        # Codigo de error
        errorCodeBk = 0

        # Comprobar si contiene un error heredado
        if errorResp is not None:
            errorCode = errorResp.get('errorCode', -99)  # Error generico

        try:
            errorCodeBk = errorCode
            # Prevenir el paso de string como error
            errorCode = int(errorCode)
        except Exception:
            self.logs.error(
                'Ocurrio une error al con el CAST del error code: {}'.format(
                    errorCodeBk
                )
            )
            errorCode = -99

        # Validar si ocurrio un error
        if errorCode < 0 or errorCode > 0:
            # Mensaje del error
            message_error = ""

            # Registro del error
            reg_error = self.___getErrorId(errorCode)

            if HTTP is not None:
                message_error = HTTP.get('message', '')

            if isinstance(reg_error, dict) and message_error == '':
                message_error = reg_error.get('message', "")
            else:
                if message_error == '':
                    message_error = reg_error

            # Estructura del mensaje de error
            dataResponse['response'] = {
                "data": {
                    "response": {
                        "hostname": self.__HOSTNAME,
                        "code": errorCode,
                        "errorCode": errorCode,
                        # Regresar el mensaje que se proporciono desde el
                        # worker que se consumio
                        "userMessage": message_error if errorResp is None else errorResp.get('userMessage')
                    }
                },
                "meta": {
                    "id_transaction": dataResponse['metadata']['id_transaction'],
                    "status": 'ERROR' if errorCode < 0 or errorCode > 0 else 'SUCCESS'
                }
            }

        # Asignar el tiempo
        dataResponse['metadata']['time'] = datetime.datetime.utcnow().isoformat()

        # Indicar el worker
        # dataResponse['metadata']['worker'] = data['metadata']['owner']

        # Topico
        dataResponse['metadata']['owner'] = self.__TOPIC__

        # Tipo de salida
        dataResponse['metadata']['mtype'] = 'output'

        # Indicar Hostname
        dataResponse['metadata']['hostname'] = self.__HOSTNAME

        # Indicar version del framework
        dataResponse['metadata']['framework'] = version

        # Validar que tenga un uowner
        if dataResponse['metadata'].get("uowner", None) is not None:
            dataResponse['metadata']['uworker'] = data['metadata']['uowner']

        # Validar si tiene asignado un uworker
        if dataResponse['metadata'].get("uworker", None) is not None:
            dataResponse['metadata']['uowner'] = data['metadata']['uworker']

        if span:
            carrier = {}
            self.___Tracer.inject(span, Format.TEXT_MAP, carrier)
            dataResponse['metadata']['span'] = carrier

        # Ver si es un Worker
        if isinstance(self.__APP, Worker):
            dataResponse['metadata']['bifurcacion'] = False

        # Ver si es bifurcacion
        is_forks = getItemBy('metadata.bifurcacion', dataResponse)

        # Metodo original
        original_method = getItemBy('metadata.original_method', dataResponse)

        '''
            Validacion del Callback
        '''

        if is_forks:
            if getItemBy('metadata.callbacks', dataResponse) is None:
                # Agregar
                dataResponse['metadata'].update({'callbacks': []})
                # Validar que sea NATS
                if nats_route is not None or len(nats_route) > 0:
                    # Agregar el Nats_UUID
                    dataResponse['metadata']['callbacks'].append({
                        "method": 'ENDPOINT',
                        "name": nats_route,
                        "version": None,
                    })

            # Recuperar los callbacks
            callbacks = getItemBy('metadata.callbacks',
                                  copy.deepcopy(dataResponse))

            if len(callbacks) > 0:
                cllbs = callbacks.pop()

                tp = cllbs.get('name')
                mt = cllbs.get('method')

                if self.__TOPIC__ == tp and original_method == mt:
                    self.logs.warning('Repetition event prevented')
                else:
                    # Agregar al arreglo
                    dataResponse['metadata']['callbacks'].append({
                        "method": original_method,
                        "name": self.__TOPIC__,
                        "version": self.__OPT.Version,
                    })
            else:
                # Agregar al arreglo
                dataResponse['metadata']['callbacks'].append({
                    "method": original_method,
                    "name": self.__TOPIC__,
                    "version": self.__OPT.Version,
                })

            # TOPIC_RESP = dataResponse['metadata']['worker']
        else:
            callbacks = getItemBy('metadata.callbacks', dataResponse)
            # Se obtiene el ultimo elemento agregado para responder a el
            if callbacks and len(callbacks) > 0:
                cllbs = callbacks.pop()
                sizeCllbs = len(callbacks)
                mthd = original_method
                find = True
                counter = 0
                if self.__TOPIC__ == cllbs.get('name') and mthd == cllbs.get('method'):
                    while find and counter < sizeCllbs:
                        cllbs = callbacks.pop()
                        if self.__TOPIC__ != cllbs.get('name') or mthd != cllbs.get('method'):
                            TOPIC_RESP = cllbs.get('name')
                            VERSION_RESP = cllbs.get('version', None)
                            dataResponse['metadata']['method'] = cllbs.get(
                                'method')
                            find = False
                        counter += 1
                else:
                    TOPIC_RESP = cllbs.get('name')
                    VERSION_RESP = cllbs.get('version', None)
                    dataResponse['metadata']['method'] = cllbs.get('method')

            else:
                pass

            original_method = dataResponse['metadata'].get(
                'original_method', None)

            # Eliminamos elemento (Este es agregado cuando es una bifurcacion)
            if original_method is not None:
                del dataResponse['metadata']['original_method']
            else:
                pass

        # Ver si es utilizado Nats
        if nats_route is None:
            OWNER = dataResponse['metadata']['owner']
            # Topico de respuesta
            TOPIC_RESP = TOPIC_RESP if TOPIC_RESP else "respuesta_{}".format(
                OWNER)
        else:
            # Validar si existe un topico seleccionado
            if TOPIC_RESP is None:
                TOPIC_RESP = nats_route
            else:
                # Comprobar si corresponde a un topico de servicio o sistema
                is_topic = re.search(
                    "[a-zA-Z0-9]{1,}[_.]{1}[a-zA-Z0-9]{1,}[_.]{1}.*",
                    TOPIC_RESP
                )

                # Validacion para la version 1 de los servicios legados
                if not is_topic:
                    if VERSION_RESP is not None:
                        is_topic = True

                # Asignar el id de ruta que proviene del reply
                TOPIC_RESP = TOPIC_RESP if is_topic else nats_route

        # Consultar quien es el callback
        cllb = getItemBy('metadata.callback', dataResponse)

        # Datos requeridos para seleccion de topic
        req_worker = getItemBy('metadata.worker', dataResponse)
        req_owner = getItemBy('metadata.owner', dataResponse)
        req_bifurcacion = getItemBy('metadata.bifurcacion', dataResponse)

        if req_worker != req_owner and req_bifurcacion:
            TOPIC_RESP = req_worker
            dataResponse['metadata']['worker'] = req_owner
            Resp = dataResponse
        elif self.__TOPIC__ != cllb and req_bifurcacion:
            TOPIC_RESP = TOPIC_RESP if TOPIC_RESP else cllb
            dataResponse['data'] = dataResponse['response']['data']
            Resp = dataResponse
        else:
            # Asignar la respuesta
            Resp = {
                "_id": dataResponse['metadata']['id_transaction'],
                "data": getItemBy('metadata.data_inicial', dataResponse),
                "headers": dataResponse.get('headers', {}),
                "metadata": dataResponse.get('metadata', {}),
                "response": dataResponse.get('response', {})
            }

        callbacksTop = getItemBy('metadata.callbacks', dataResponse)

        if callbacksTop and len(callbacksTop) == 0:
            del Resp['metadata']['metadata_inicial']
            del Resp['metadata']['data_inicial']

        id_transaction = dataResponse['metadata']['id_transaction']

        # Revisar si es un smoketest
        if isSmokeTest:
            # respuesta generica
            Resp.update(
                {
                    'response': {
                        "data": {
                            "response": {
                                "code": errorCode,
                                "framework": f'mscoppel:{version}',
                                "hostname": self.__HOSTNAME,
                                "userMessage": "Smoke test"
                            }
                        },
                        "meta": {
                            "id_transaction": id_transaction,
                            "status": 'ERROR' if errorCode < 0 or errorCode > 0 else 'SUCCESS'
                        }
                    }
                }
            )

        span.log_kv({'event': 'output', 'response': Resp['response']})

        if nats_route is not None:
            Resp.update({"uuid": nats_route})

        # Validar si se envia el objeto HTTP perzonalizado
        if HTTP is not None:
            Resp['metadata'].update({
                "http": HTTP
            })
        else:
            # Datos por default
            Resp['metadata'].update({
                "http": {
                    "code": 500 if errorCode < 0 or errorCode > 0 else 200,
                    "headers": {},
                    "message": ""
                }
            })

        # Validar si es modeo DEBUG
        if self.__OPT.Debug and self.__fluentd_logs:
            self.logs.info("SALIDA [{}]".format(TOPIC_RESP), Resp)
        elif self.__OPT.Debug:
            self.logs.info("SALIDA [{}]: {}".format(
                TOPIC_RESP, json.dumps(Resp)))

        # Publicar la respuesta
        self._send(TOPIC_RESP, Resp,
                   dataResponse['metadata']['id_transaction'])

    def ___getErrorId(self, id_error):
        """
            Metodo apra recuperar los datos de un error registrado
        """
        try:
            if self.__ERRORS.get(str(id_error), None):
                return self.__ERRORS.get(str(id_error))
            else:
                return self.__ERRORS.get('-99')
        except Exception:
            self.logs.error(
                'Ocurrio un excepcion al recuperar los datos del error')
            return 'Ocurrio un error generico'

    def __getListener(self):
        """
            Metodo para recuperar la accion registrada
            en el Listener, de no contar con dicha accion
            se lanza una excepcion.
        """
        if self.__ACTIONS.get(TypesActions.LISTENER, None):
            return self.__caller(self.__ACTIONS.get(TypesActions.LISTENER))
        else:
            raise Exception('No existe un metodo para el evento solicitado')

    def __getMethod(self, meta):
        """
            Metodo para recuperar el metodo que se
            ejecutara, segun los datos pasados en
            el metada.
        """
        # Recuperar el metodo
        METHOD = meta.get('method', None)

        # Metodo que se ejecutara
        FNC = None

        # Seleccion del metodo
        if METHOD:
            if (meta.get('uuid', None) and len(meta.get('uuid', '')) > 0):
                if METHOD == 'GET':  # Consultar un elemento
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.GET))
                elif METHOD == 'DELETE':  # Eliminar el elemento
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.DELETE))
                elif METHOD == 'PUT':  # Actualizar el elemento
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.UPDATE))
            elif METHOD == 'GET':  # Lista de servicios
                ruta_health_check = os.path.basename(meta.get('ruta', ''))
                if ruta_health_check == 'liveness' and self.__ACTIONS.get(TypesActions.LIVENESS, None):
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.LIVENESS))
                elif ruta_health_check == 'readness' and self.__ACTIONS.get(TypesActions.READNESS, None):
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.READNESS))
                elif self.__ACTIONS.get(TypesActions.LIST, None):
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.LIST))
            elif METHOD == 'POST':  # Creacion de un nuevo elemento
                if self.__ACTIONS.get(TypesActions.CREATE, None):
                    FNC = self.__caller(self.__ACTIONS.get(TypesActions.CREATE))

        # Validar si se recuper un metodo asignado
        if FNC is not None:
            return FNC
        else:
            # Regresar el Listener por default
            return self.__getListener()

    def __initActions(self):
        """
            Metodo que se encarga de recuperar todas las
            acciones registradas, para su implementacion
            durante su llamado.
        """
        for f in inspect.getmembers(self):
            # Validar que tenga el atributo minimo
            if hasattr(f[1], '__MICROSERVICE_ACTION__'):
                # Recuperar el tipo de accion
                typeAction = getattr(f[1], '__TYPE__')
                # Validar si es el de errores
                if typeAction == TypesActions.ERRORS:
                    # Recuperar la funcion para ejecutarla
                    errorFNC = self.__caller(f[0])
                    # Ejecutar la funcion para recuperar los errores definidos
                    errores_definidos = errorFNC()
                    # Validar que sea el tipo correcto
                    if not isinstance(errores_definidos, dict):
                        self.logs.error(
                            'No se proporciono un formato correcto para los errores definidos')
                        sys.exit(-1)
                    # Ejecutar y asignar los errores
                    self.__ERRORS = self.__merge_dicts(
                        self.__ERRORS, errores_definidos)
                elif typeAction == TypesActions.FORKS:
                    # Recuperar la funcion para ejecutarla
                    forksFNC = self.__caller(f[0])
                    # Ejecutar la funcion para recuperar la configuracion
                    forks_conf = forksFNC()
                    # Validar que sea el tipo correcto
                    if not isinstance(forks_conf, dict):
                        self.logs.error(
                            'No se proporciono un formato correcto para los Forks definidos')
                        sys.exit(-1)
                    # Validar que cuente con el forma correcto
                    valid, error = validForksConf(forks_conf)
                    if valid:
                        self.__FORKS = forks_conf
                    else:
                        self.logs.error(error)
                        sys.exit(-1)
                else:
                    # Almacenar la accion
                    self.__ACTIONS.update({typeAction: f[0]})

    def __caller(self, name):
        """
            Metodo para recuperar una propiedad que sera utilizada
            como metodo
        """
        if hasattr(self, name):
            return getattr(self, name)

    @ abstractmethod
    def smoketest(self):
        """
            Metodo que es llamado para validar los
            servicios desde su consumo por REST/Kafka
        """
        pass

    def __merge_dicts(self, *dict_args):
        """
        Given any number of dicts, shallow copy and merge into a new dict,
        precedence goes to key value pairs in latter dicts.
        """
        result = {}
        for dictionary in dict_args:
            result.update(dictionary)
        return result

    def __getConfServiceFork(self, conf):
        """
            Metodo para iniciar la configuracion de los Forks a ejecutar
        """
        forks = []

        # Recorrer las configuraciones
        for item in conf:
            # Verificar que existe en las configuraciones
            if self.__FORKS.get(item.get('fork'), None) is None:
                self.logs.error(
                    'No se encuentra configurado el Fork {}'.format(item.get('fork')))
                raise Exception(
                    'No se encuentra configurado el Fork {}'.format(item.get('fork')))

            # Actualizar el item con la configuracion inicial
            item.update({"conf": self.__FORKS.get(item.get('fork', ''), None)})

            # Validar si es necesario recuperar una funcion
            if not item.get('fnc', None) is None:
                item.update({'fnc_call': self.__caller(item.get('fnc'))})

            # Agregar a la coleccion
            forks.append(item)

        # Regresar la coleccion
        return forks
