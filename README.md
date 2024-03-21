# Informe TP0 - Sistemas Distribuidos - 1c2024

---

<br>
<p align="center">
  <img src="https://raw.githubusercontent.com/MiguelV5/MiguelV5/main/misc/logofiubatransparent_partialwhite.png" width="60%"/>
</p>
<br>

---

<br>
<p align="center">
<font size="+3">
Miguel Angel Vasquez Jimenez - 107378
</font>
</p>
<br>

---

<br>

A continuación se presentan aclaraciones y notas generales acerca del trabajo practico.

##  Navegacion por los ejercicios

Para facilitar la navegación por la solución de los distintos ejercicios, se dividieron los commits en distintas ramas, cada una correspondiente a un ejercicio o subejercicio. Tomar el commit final de cada rama como la solución final del ejercicio correspondiente.
Cada rama surge de la rama de su ejercicio predecesor.

## Ejercicios 1 y 1.1

Se realizó un script de bash para poder parametrizar la cantidad de clientes que se quieren definir en el archivo `docker-compose-dev.yaml` sin tener que modificarlo manualmente.
El mismo reescribe el archivo docker-compose-dev.yaml con la cantidad que se le solicite. 

Ejecución del script:

```bash
./create-multiclient-docker-compose.sh <number_of_clients>
```

## Ejercicio 2

Se modifica el script `create-multiclient-docker-compose.sh` y se genera nuevamente el `docker-compose-dev.yaml` para añadir volumenes (tanto para server como para clientes) de tal forma que los containers puedan utilizar los archivos de configuracion desde el host sin necesidad de estar ligados al build de las imagenes en sí.

Dichos volumenes son declarados con la opción de solo lectura `ro`, e incluyen exclusivamente a los archivos de configuración correspondientes.

Para la ejecución y testeo:
- Se comenta temporalmente el target `docker-image` en el makefile.
- Se ejecuta el target:

```bash
make docker-compose-up
```

- Se cambian las configuraciones y se ejecuta nuevamente para ver reflejados los cambios sin haber hecho build nuevamente.

## Ejercicio 3

Para el test se define una nueva imagen `netcat-tester` que utilice el script `sv-test.sh` en su correspondiente contenedor, que se comunique con el servidor a traves de la misma network definida en `docker-compose-sv-test.yaml`.

Se añadieron adicionalmente los targets `netcat-sv-test-up` y `netcat-sv-test-down` al makefile para correr la prueba facilmente de la forma:

```bash
make netcat-sv-test-up
```

## Ejercicio 4

Se agrega el manejo de señales para `client/common/client.go` y `server/common/server.py` por medio de la recepción de SIGTERM para el cierre adecuado de file descriptors. Particularmente se decidió cerrar los sockets de comunicación al mismo momento de recibir la señal, y adicionalmente para el server, se dejan de aceptar clientes que soliciten conectarse tras haber recibido la misma. 

Para la verificación de funcionamiento:
- Iniciar la comunicación:

```bash
make docker-compose-up

```
- Enviar la señal:

```bash
make docker-compose-down
```

- Observar los logs:

```bash
make docker-compose-logs
```



