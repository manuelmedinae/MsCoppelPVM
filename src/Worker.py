from .Base import Base
from .ErrorMs import ErrorMs
from .HttpResponse import HttpResponse


class Worker(Base):
    """
        Clase que contiene la logica para el manejo de los
        worker.
    """

    def process(self, span, request, fnc, params=[]):
        """
            Metodo que se encarga de procesar, la accion
            que se asign al evento del microservicio.

            @params resquest peticion de datos
            @params fnc Funcion a ejecutar
        """
        try:
            # Parametros
            paramsInject = self.getParams(span, request, params)

            # Ejecutar la funciona final
            RESP = fnc(*paramsInject)
            # Regresar la respuesta con el formato correcto
            return self.formatResponse(request, RESP)
        except ErrorMs as err:
            # Retornar el error
            return self.formatResponse(
                request,
                # Generar formato
                HttpResponse(None, err.http_code,
                             headers=err.headers, message=err.message),
                err.errorCode
            )
