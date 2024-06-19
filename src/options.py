import os
import sys
import re
from .loggs import Loggs


class Options:
    App = None
    Debug = False
    Hosts = []
    Name = None
    Public = False
    Version = None
    Legacy = False
    Logger = Loggs('Options')
    isNats = False

    def __init__(self, app, name, version, hosts=[], is_nats=False):
        """
            Clase para la construccion de las opciones del microservicio.

            @params app Nombre de la aplicacion
            @params name Nombre del microservicios
            @params version Numero de la version del microservicio
            @params hosts Lista de direcciones de kafka o Nats.
            @params public Indicador si el servicio es de accesso publico

            @returns void
        """
        # Validar que se pase la direccion de kafka de forma correcta
        if not isinstance(hosts, list):
            self.Logger.error(
                'No se proporciono una lista de direcciones de kafka correcta')
            sys.exit(-1)
        elif len(hosts) < 1:
            self.Logger.error('Se proporciono una lista vacia de Kafka Hosts')

        # Asignar la variable debug a verdadero si no es productivo.
        self.Debug = False if os.environ.get('PRODUCTION', False) else True

        # Asignar el nombre de la aplicacion
        self.App = app

        # Asignar el nombre del microservicio
        self.Name = name

        # Asignar la version
        self.Version = version

        # Validar si es Nats
        if os.environ.get('REST_ENABLED', None) is None:
            if os.environ.get('NATS', None) is not None:
                if len(os.environ.get('NATS')) > 0:
                    self.isNats = True
                else:
                    self.isNats = False
            elif len(re.findall(r'^nats.*', hosts[0])) > 0:
                self.isNats = True
            else:
                self.isNats = False
        else:
            pass

        # Revalidar los brokers
        if len(os.environ.get('NATS', '')) > 0:
            self.Hosts = os.environ.get('NATS', '').split(',')
        elif len(os.environ.get('KAFKA', '')) > 0:
            self.Hosts = os.environ.get('KAFKA', '').split(',')
        else:
            # Asignar el cluster de kafka
            self.Hosts = hosts
