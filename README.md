# Librería MSCoppelPVM 

## MsCoppel 
Paquete para implmentar microservicios basados en mensajes, utilizando kafka, para su implementacion se utiliza decoradores sobre las funciones, dependiendo de las acciones que se quieren implementar.

### Instalacion
```console
    $ pip install MsCoppelPVM
```

### Métodos disponibles
* List
* Get
* Create
* Update
* Delete

En caso de que no se definan mas elementos todos las peticiones siempre seran escuchadas por el metodo **Listener**.

### Ejemplo
```python
    from MsCoppel import Microservices, Options, Types, MsManager, ErrorMs


@Manager.Define(
    Options(
        'demo',  # Nombre de la aplicacion
        'plantilla',  # nombre del microservicio
        'v1',  # Version del microservicio
        ['kafka:9092'],  # Coleccion de servidores
        Types.WORKER, True
    )
)
class Demo(microservices):

    def smoketest(self):
        """
            Metodo que se utiliaza para validar el servicio
            desde una consulta REST.
 
            su valor de retorno siempre es logico.
        """
        True

    @MsManager.Errors
    def misErrores(self):
        return {'-12': 'Error definido por el usuario'}

    @MsManager.List
    def listar():
        pass

    @MsManager.Get
    def cosa(self, data, auth, id):
        raise ErrorMs(-12)

    @MsManager.Create
    def nuevo(self, data, auth):
        pass

    @MsManager.Update
    def actualizar(self, data, auth):
        pass

    @MsManager.Delete
    def eliminar(self, data, auth):
        pass

    @MsManager.Listener
    def lister(self, data, auth):
        return data


    @MsManager.HealthCheckLiveness([], ctx=['data'])
    def get_healthcheck_liveness(self, params):
        pass
    
    @MsManager.HealthCheckReadness([], ctx=['data'])
    def get_healthcheck_readness(self, params):
        pass
```

### Errores
Para retornar un error se utiliza el estandar de python que es **raise** regresando un objeto error perzonalizado de la clase **ErrorMs**, tal como se muestra en el siguiente ejemplo

```python
@MsManager.Get
    def cosa(self, data, auth, id):
        raise ErrorMs(-12)
```

> Tome en cuenta que solo se indica el numero del error previamente registrados por el decorador **@MsManager.Errors**.