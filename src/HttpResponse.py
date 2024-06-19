class HttpResponse:
    """
        Clase base para respuestas dentro de un servicio HTTP
    """
    # Datos que seran enviados
    data = {}

    # Codigo que sera enviado
    code = 200

    # Mensaje que se quiere pasar
    message = ""

    # Headers
    headers = {}

    def __init__(self, data, code, message="", headers={}):
        """
            Constructor de la clase para la respuesta de HTTP
        """
        self.data = data
        self.code = code
        self.message = message
        self.headers = headers
