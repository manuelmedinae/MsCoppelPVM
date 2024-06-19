import time
import math
import datetime
from .Base import Base
from .Util import getItemBy, verbAction
from .HttpResponse import HttpResponse


class Fork(Base):

    # Configuracion de los forks
    ___conf_forks = {}

    # Forks
    ___forks = []

    # Primer bifurcacion
    ___isFirstFork = False

    def process(self, span, data, fnc, params=[]):
        """
            Metodo que se encarga de realizar le procesamiento de
            los mensajes pasados por Microservices
        """

        # Posicion del actual Fork
        worker = self.nextFork(getItemBy('metadata.owner', data))
        # Agregar el tiempo actual
        data['metadata']['time'] = datetime.datetime.utcnow().isoformat()
        # Indicar tipo
        data['metadata']['mtype'] = 'input'
        # Validar si tiene asignado la metadata original
        if getItemBy('metadata.metadata_inicial', data) is None:
            data['metadata']['metadata_inicial'] = data['metadata'].copy()

        # Validar si tiene almacenada los datos originales
        if getItemBy('metadata.data_inicial', data) is None:
            data['metadata']['data_inicial'] = data['data'].copy()

        owner = getItemBy('metadata.owner', data)

        # Validar si es consecutivo o primer servicio
        if self.Topico != owner or len(self.___forks) < 1:
            # Recuperar el siguiente
            nextFork = self.nextFork(getItemBy('metadata.owner', data))

            # Validar si ocurrio un error
            isError = self.isError(data)

            # Validar si aun se tienen elementos que procesar (Forks)
            if nextFork is None or isError:
                data['metadata']['bifurcacion'] = False
            else:
                data['metadata']['bifurcacion'] = True

            if isError:
                # Retornar el mensaje correcto
                return self.formatResponse(
                    data,
                    # Enviar el resultado correcto
                    self.get_http_data(data['metadata']),
                    self.getErrorCode(data),
                    self.getErrorMeta(data)
                )
            elif nextFork is None:
                # Parametros
                paramsInject = self.getParams(span, data, params)

                # Ejecutar la funciona final
                RESP = fnc(*paramsInject)

                # Regresar la respuesta con el formato correcto
                return self.formatResponse(data, RESP)
            else:
                row = self.getNextTopic(nextFork)
                conf = self.___conf_forks.get(row.get('current'))
                method = data['metadata']['method']

                data['metadata']['original_method'] = method
                data['metadata']['method'] = verbAction(conf.get('action'))

                resp = None

                r1 = getItemBy('response', data)
                r2 = getItemBy('response.data', data)
                r3 = getItemBy('response.data.response', data)

                if r1 and r2 and r3:
                    resp = getItemBy('response.data.response', data)

                data['data'] = self.getData(
                    getItemBy('metadata.data_inicial', data),
                    conf,
                    resp,
                )

                data = self.changeMetadata(data, row.get('current'))

                return self.formatResponse(data, None, 0)

        else:
            # Recuperar los datos requeridos del siguiente Topico
            row = self.getNextTopic(worker)

            # Recuperar la configuracion
            conf = self.___conf_forks.get(row.get('current'), None)

            # Indicar que es una bifurcacion
            data['metadata']['bifurcacion'] = True

            # Agregar el metodo que corresponde y se
            # guarda una copia para usarla despues
            data['metadata']['original_method'] = data['metadata']['method']
            data['metadata']['method'] = verbAction(conf.get('action'))

            # Recuperar la configuracion
            data['data'] = self.getData(
                data['data'],
                conf,
                None,
            )

            # Aplicar los cambios
            data = self.changeMetadata(data, row.get('current'))

            # Retornar la respuesta
            return self.formatResponse(data, None, 0)

    def confForks(self, conf):
        """
            Metodo para registrar la configuracion del metodo
            que se ejecutara.
        """
        if conf is not None:
            # Asignar los paso a repetir.
            for item in conf:
                # Configuracion
                conf_item = item.get('conf')
                if not self.existService(conf_item):
                    Version = conf_item.get('Version')
                    # Validar el formato del ID
                    if conf_item.get('App') == 'appcoppel' and Version == 'v1':
                        self.___conf_forks.update(
                            {conf_item.get('Name'): item})
                    else:
                        # Id basado en la version 2
                        id_unique = '{}_{}_{}'.format(
                            conf_item.get('App'),
                            conf_item.get('Version'),
                            conf_item.get('Name'),
                        )
                        self.___conf_forks.update({id_unique: item})

                    # Coleccion de forks para la accion.
                    self.___forks.append(item.get('conf', None))

    def existService(self, conf):
        """
            Metodo para validar si nu servicio ya existe
            previamente registrado.
        """
        for item in self.___forks:
            APP = item.get('App')
            NAME = item.get('Name')
            if APP == conf.get('App') and NAME == conf.get('Name'):
                return True

        return False

    def getData(self, dataDefault, conf, response):
        """
            Metodo para recuperar el data que se ejecutara

            @param dataDefault Data por default
            @param conf Configuracion del fork
            @param response Datos del servicio previo
        """
        if conf.get('fnc_call', None):
            # Recuperar la funcion
            fnc_call = conf.get('fnc_call')
            # Ejecutar la funcion los parametros de
            # los datos nuevos y originales
            resp = fnc_call(dataDefault, response)
            # Validar que sea un diccionario
            if isinstance(resp, dict):
                return resp
            else:
                # Wrap para no ocacionar error
                return {"value": resp}
        elif conf.get('data', None):
            return conf.get('data')
        else:
            return dataDefault

    def getNextTopic(self, worker):
        """
            Metodo para crear la estructura para la publicacion del mensaje.
            @param worker Configuracion del worker
        """
        # Generar el elemento actual
        current = '{}_{}_{}'.format(
            worker.get('App'),
            worker.get('Version'),
            worker.get('Name'),
        )
        if worker.get('Version') == 'v1' and worker.get('App') == 'appcoppel':
            current = worker.get('Name')

        return {'current': current, 'name': worker.get('Name')}

    def nextFork(self, nameFork):
        """
            Metodo para buscar la posicion del siguiente Fork a ejecutar

            @params nameFork Nombre que se busca
        """
        # Ver el tamaño las bifurcaciones asignadas
        size = len(self.___forks)
        if size == 0:
            return None

        # Ver si no corresponde al propio topico
        if nameFork == self.Topico:
            self.___isFirstFork = True
            return self.___forks[0]

        # Buscar la posicion del elemento actual
        for index in range(0, size):
            obj = self.___forks[index]
            rpTopic = self.getNextTopic(obj)
            # Validar si es el que corresponde
            if rpTopic.get('current') == nameFork:
                if (index + 1) < size:
                    return self.___forks[index+1]
                else:
                    return None

        # Valor por default
        self.___isFirstFork = True
        return self.___forks[0]

    def get_http_data(self, meta, data=None):
        """
            Metodo para recuperar los datos HTTP de una worker previo
        """
        if "http" in meta:
            HTTP = meta["http"]
            return HttpResponse(
                data,
                HTTP["code"],
                HTTP["message"],
                HTTP["headers"]
            )
        else:
            return data

    def isError(self, data):
        """
            Metodo para asergurar si ocurrio un error al
            momento de consumir el servicio
        """
        r = getItemBy('response', data)
        rp = getItemBy('response.data.response', data)
        ec = False if getItemBy(
            'response.data.response.errorCode', data) is None else True
        return r and rp and ec

    def getErrorCode(self, data):
        """
            Metodo para recuperar el error code
        """
        # Recuperar el codigo de error
        errorCode = getItemBy('response.data.response.errorCode', data)
        # Validar que se proporcionara un codigo de error.
        if errorCode is None:
            # Valor por default
            return -99
        else:
            return errorCode

    def getErrorMeta(self, data):
        """
        Metodo para recuperar la meta data de los errore.

        Arguments:
            data object -- Datos de la repuesta
        """
        # Recuperar to do el objeto de error.
        errorCode = getItemBy('response.data.response.errorCode', data)

        # Validar que sea un error y no un dato correcto.
        if errorCode is None:
            return None
        else:
            return getItemBy('response.data.response', data)

    def changeMetadata(self, data, current):
        """
            Metodo apra aplicar los cambios sobre la metadata
        """
        data['metadata']['id_operacion'] = math.floor(time.time()) * 433333333
        data['metadata']['uowner'] = getItemBy('metadata.uworker', data)
        data['metadata']['worker'] = current  # Worker que procesara
        data['metadata']['uworker'] = '{}_{}'.format(
            getItemBy('metadata.worker', data),
            getItemBy('metadata.id_operacion', data),
        )
        data['metadata']['owner'] = self.Topico
        # Regresar los nuevos datos
        return data
