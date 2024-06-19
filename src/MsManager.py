"""
    Modulo para la implementacion de los decoradores,
    necesarios para el uso de reflec data, al momento
    de registrar las acciones de los microservicios.
"""

import inspect
import sys
from .types import TypesActions
from .loggs import Loggs

logs = Loggs('Core')


def validForm(forkConf):
    """
        Metodo para realizar la validacion del diccionario
        de configuracion de implementacion un Worker.
    """
    if len(forkConf) > 0:
        for conf in forkConf:

            invalid = False

            # Validar que cuente con los elementos minimos
            if conf.get('fork', None) is None:
                invalid = True

            if invalid:
                logs.error(
                    'La configuracion para la bifurcacion no es correcta')
                sys.exit(-1)


def validParam(fn):
    """
        Metodo para validar que se proporcionaran
        los parametros minimos para la funciones
        que interactuan con un registro expecifico.
    """
    if len(inspect.getargspec(fn)[0]) < 4:
        logs.error(
            'Lo metodos GET, DELETE, UPDATE requieren 3 parametros')
        sys.exit(-1)


def asignDec(fn, typeDec):
    """
        Metodo para agregar el decorador y los atributos
        necesarios para el reflect data
    """
    # Decorador de la funcion
    def decorated(*args, **kwargs):
        return fn(*args, **kwargs)

    # Asignar los atributos
    if typeDec == TypesActions.ERRORS or inspect.isfunction(fn):
        setattr(decorated, '__MICROSERVICE_ACTION__', True)
        setattr(decorated, '__TYPE__', typeDec)
        setattr(decorated, '__FUNC_NAME__', fn.__name__)
    return decorated


def Define(opt):
    """
        Metodo que agrega la informacion
        necesaria para iniciar la clase del
        microservicio.
    """
    def dec(cls):
        try:
            cls(opt)
        except KeyboardInterrupt:
            cls.logs.info('Keyboard Interrupt')
        except RuntimeError:
            cls.logs.info('Runtime Error')
        return cls
    return dec


def List(forks=[], ctx=[]):
    """
        Decorador para las acciones de tipo
        LIST (GET a la raiz del servicio)
    """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.LIST)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        return afn
    return x


def Get(forks=[], ctx=[]):
    """
        Decorador de la accion cuando se solicitan
        los datos de un elemento especifico
    """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.GET)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn
    return x

def HealthCheckLiveness(forks=[], ctx=[]):
    """
           Decorador de accion de creacion de un
           elemento en el servicio
       """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.LIVENESS)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn

    return x

def HealthCheckReadness(forks=[], ctx=[]):
    """
           Decorador de accion de creacion de un
           elemento en el servicio
       """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.READNESS)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn

    return x


def Create(forks=[], ctx=[]):
    """
        Decorador de accion de creacion de un
        elemento en el servicio
    """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.CREATE)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn
    return x


def Update(forks=[], ctx=[]):
    """
        Decorador de actualizacion de un
        servicio en especfico
    """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.UPDATE)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn
    return x


def Delete(forks=[], ctx=[]):
    """
        Decorador de accion de eliminacion
        de un elemento especfico
    """
    validForm(forks)

    def x(fn):
        # Decorador original
        afn = asignDec(fn, TypesActions.DELETE)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        # Retornar el decorador
        return afn
    return x


def Listener(forks=[], ctx=[]):
    """
        Decorador Generico
    """
    validForm(forks)

    def x(fn):
        # Retornar el decorador
        afn = asignDec(fn, TypesActions.LISTENER)
        # Asginar los datos
        if len(forks) > 0:
            setattr(afn, '__CONF_FORKS__', forks)
        # Configuracion de parametros
        if len(ctx) > 0:
            setattr(afn, '__CONF_PARAMS__', ctx)
        return afn
    return x


def Errors(fn):
    """
        Decorador para asignacion de los errores
    """
    # Retornar el decorador
    return asignDec(fn, TypesActions.ERRORS)


def Forks(fn):
    """
        Decorador para asignacion de los Forks
    """
    # Retornar el decorador
    return asignDec(fn, TypesActions.FORKS)
