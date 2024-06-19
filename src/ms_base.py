import base64
import json
import os
import sys
import _thread
from kafka import KafkaConsumer, KafkaProducer
from .loggs import Loggs


class KafkaBase:
    """
        Base del microservicio que se aplica para
        la comunicacion con kafka
    """
    # Topico de conexion
    __TOPIC = ''
    # Direccion de kafka
    __KAFKAHOSTS = ''
    # Instancia del consumer
    __CONSUMER = None
    # Instancia del producer
    __PRODUCER = None
    # Logs de la aplicacion
    logs = Loggs('Service')

    def __init__(self, topic, kafkaHosts, name=None):
        self.logs.info('Iniciando la conexion los servidores de KAFKA')
        # Asignar el topico de conexion
        self.__TOPIC = topic
        # Asginar el hosts de kafka
        self.__KAFKAHOSTS = kafkaHosts
        # Conectar a Kafka Producer
        self.__connectProducer()
        # Conectar a Kafka Consumer
        self.__connectConsumer()

    def __connectConsumer(self):
        """
            Metodo para realizar la conexion al cosumer de kafka.
        """
        topics = [self.__TOPIC, "validateConnection"]

        CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP', self.__TOPIC)

        # Conexion a kafka
        try:
            self.__CONSUMER = KafkaConsumer(
                *topics,
                group_id=CONSUMER_GROUP,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                bootstrap_servers=self.__KAFKAHOSTS,
                value_deserializer=self.__b64_to_json
            )
        except Exception as e:
            self.logs.error('Ocurrio un error al conectar con Kafka')
            self.logs.error(e)
            sys.exit(-1)

        # Notificar que se encuentra conectado
        self.logs.info("Escuchando el topico {} en el Kafka {}".format(
            self.__TOPIC, ','.join(self.__KAFKAHOSTS)))

        # Escuchar todos los posibles eventos
        for msg in self.__CONSUMER:
            try:
                # Solo procesar los mensajes del topico
                if msg.topic == self.__TOPIC:
                    _thread.start_new_thread(self._message, (msg.value,))
                    # self._message(msg.value)
            except Exception as e:
                print(e)

    def _message(self, msg):
        """
            Metodo para el procesamiento de mensajes de
            kafka.
        """
        pass

    def __connectProducer(self):
        """
            Metodo para realizar la conexion con el producer
            de kafka.
        """
        try:
            self.__PRODUCER = KafkaProducer(
                bootstrap_servers=self.__KAFKAHOSTS,
                value_serializer=self.__json_to_b64
            )
            self.logs.info('Conectado a kafka para enviar mensajes')
        except Exception as e:
            self.logs.error('Ocurrio un error al conectar con el producer')
            self.logs.error(e)
            sys.exit(-1)

    def __json_to_b64(self, json_in):
        """
            Metodo que pasa un objecto a el formato necesario
            para su comunicacion
        """
        return base64.b64encode(str.encode(json.dumps(json_in)))

    def __b64_to_json(self, encoded):
        """
            Metodo que conviernte un base64 a dict
        """
        decoded = base64.b64decode(encoded)
        return json.loads(decoded.decode('utf-8'))

    def _send(self, topico, msj, idTransaction):
        """
            Metodo para el envio de datos a Kafka

            @params topico Topico en el que se publica
            @params msj Mensaje que enviara
            @params idTransaction Id de la transaccion
        """
        try:
            self.__PRODUCER.send(topico, key=str.encode(
                str(idTransaction)), value=msj)
        except Exception as e:
            self.logs.error(e)
            sys.exit(-1)

    def validateConnection(self):
        try:
            self.__CONSUMER.topics()
            self.__PRODUCER.send('validateConnection', '')
            return True
        except Exception as e:
            return False
