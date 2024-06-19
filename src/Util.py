from .types import Actions


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def Merge(dict1, dict2):
    return(dict2.update(dict1))


def getItemBy(anotation, data, default=None):
    """
        Metodo para acceder en anotacion de objeto a las propiedades
        de un diccionario.
    """
    sep = anotation.split('.')
    if len(sep) > 0:
        current = data
        for item in sep:
            if current.get(item, None) is None:
                return default
            else:
                current = current.get(item, default)
        # Regresar el resultado del ciclo
        return current
    else:
        # No cuenta con un solo elemento
        return default


def verbAction(action):
    """
        Metodo para recuperar el verbo de una
        accion para el envio de parametro.
    """
    if action == Actions.CREATE:
        return 'POST'
    elif action == Actions.DELETE:
        return 'DELETE'
    elif action == Actions.GET:
        return 'GET'
    elif action == Actions.LIST:
        return 'GET'
    elif action == Actions.UPDATE:
        return 'PUT'
    elif action == Actions.LIVENESS:
        return 'LIVENESS'
    elif action == Actions.READNESS:
        return 'READNESS'
    return 'GET'


def validForksConf(conf):
    """
        Metodo para validar la configuracion
        de los Forks tiene el formato
        correspondiente.
    """
    error = None

    # Validar que sea un diccionario
    if not isinstance(conf, dict):
        error = 'No se prporciono una configuracion globar de Forks Correcta'
        return False, error

    # Recorrer la configuracion para validar los metodos
    for name, confservice in conf.items():
        if not isinstance(confservice, dict):
            error = 'La configuracion del servicio {} no es correcta'.format(
                name)
            return False, error

        # Validar que cuente con los elementos basicos
        if confservice.get('App', None) is None:
            error = 'Se requiere la configuracion "APP" para el servicio "{}"'.format(
                name)
        elif confservice.get('Name', None) is None:
            error = 'Se requiere la configuracion "Name" para el servicio "{}"'.format(
                name)
        elif confservice.get('Version', None) is None:
            error = 'Se requiere la configuracion "Version" para el servicio "{}"'.format(
                name)

        if not error is None:
            return False, error

    return True, error
