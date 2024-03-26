# Informe TP0 - Sistemas Distribuidos

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
<br>
<br>
Primer cuatrimestre - 2024
</font>
</p>
<br>

---

<br>

A continuación se presentan aclaraciones y notas generales acerca del trabajo practico.


> [!NOTE] 
> Para facilitar la navegación por la solución de los distintos ejercicios, se dividieron los commits en distintas ramas, cada una correspondiente a un ejercicio o subejercicio. Tomar el commit final de cada rama como la solución final del ejercicio correspondiente.
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

Se añadieron los targets:

```bash
make netcat-sv-test-up
```

```bash
make netcat-sv-test-logs
```

```bash
make netcat-sv-test-down
```

## Ejercicio 4

Se agrega el manejo de señales para `client/common/client.go` y `server/common/server.py` por medio de la recepción de SIGTERM para el cierre adecuado de file descriptors. Particularmente se decidió cerrar los sockets de comunicación al mismo momento de recibir la señal, y adicionalmente para el server, se dejan de aceptar clientes que soliciten conectarse tras haber recibido la misma. 
Tambien se modifica el tiempo que espera docker-compose-down para enviar la señal de terminación, de tal forma que se pueda observar el cierre graceful.

Para la verificación de funcionamiento:

- Iniciar la comunicación:

```bash
make docker-compose-up
```

- Observar los logs en una terminal adicional:

```bash
make docker-compose-logs
```

- Enviar la señal:

```bash
make docker-compose-down
```

- Output:
```bash
...
client2  | time="2024-03-22 21:25:58" level=info msg="action: receive_message | result: success | client_id: 2 | msg: [CLIENT 2] Message N°1\n"
client1  | time="2024-03-22 21:26:03" level=info msg="action: socket_closing | result: success | client_id: 1"
client1  | time="2024-03-22 21:26:03" level=info msg="action: signal_receiver_channel_shutdown | result: success | client_id: 1"
client1  | time="2024-03-22 21:26:03" level=info msg="action: loop_finished | result: success | client_id: 1"
client2  | time="2024-03-22 21:26:03" level=info msg="action: socket_closing | result: success | client_id: 2"
client2  | time="2024-03-22 21:26:03" level=info msg="action: signal_receiver_channel_shutdown | result: success | client_id: 2"
client2  | time="2024-03-22 21:26:03" level=info msg="action: loop_finished | result: success | client_id: 2"
client3  | time="2024-03-22 21:26:03" level=info msg="action: socket_closing | result: success | client_id: 3"
client3  | time="2024-03-22 21:26:03" level=info msg="action: signal_receiver_channel_shutdown | result: success | client_id: 3"
client3  | time="2024-03-22 21:26:03" level=info msg="action: loop_finished | result: success | client_id: 3"
client1 exited with code 0
client2 exited with code 0
client3 exited with code 0
server   | 2024-03-22 21:26:04 INFO     action: exiting_due_to_signal | result: in_progress
server   | 2024-03-22 21:26:04 INFO     action: socket_closing | result: success
server exited with code 0
```

## Ejercicio 5

Se establece como protocolo de comunicación el intercambio de mensajes representados como strings con los siguientes formatos según tipo de mensaje:

- (Cliente -> Servidor) ***Envio de datos con apuestas***. Cada conjunto de parametros encerrado por un par de llaves representa una apuesta con sus respectivos datos separados por comas (`,`). Cada parametro es un par `clave:valor`. La finalización del mensaje completo se denota con el delimitador `;`.

<p align="center">
<code>{PlayerName:STRING,PlayerSurname:STRING,PlayerDocID:INT,PlayerDateOfBirth:STRING,WageredNumber:INT,AgencyID:INT},{ ... }, ... , { ... };</code>
</p>

<p align="center">
(En particular para este ejercicio los mensajes solo contienen una apuesta).
</p>

- (Servidor -> Cliente) ***Confirmación de apuesta recibida***. Se envia cuando la apuesta es almacenada adecuadamente en el servidor. Este mensaje mantiene el comportamiento de EchoServer, enviando el mismo mensaje del cliente como confirmación. Este tipo de mensaje se define para este ejercicio exclusivamente.

Adicionalmente se definieron las variables de entorno de la apuesta de prueba (`NOMBRE`, `APELLIDO`, `DOCUMENTO`, `NACIMIENTO` y `NUMERO`) en el archivo de configuración.
De esta forma, se puede seguir ejecutando el caso de forma sencilla:

```bash
make docker-compose-up
```

## Ejercicio 6

Se añade toda la logica de comunicación y manejo de mensajes en el servidor y cliente para poder enviar chunks de apuestas de cantidad configurable, por medio del valor de `bets_per_chunk` en `config.yaml`. A su vez se remueve la utilización de las variables `lapse` y `period` en el archivo de configuración, ya que a partir del ejercicio actual imposibilitan la comunicación completa de las apuestas dado el tamaño de los mensajes y el tiempo de espera entre ellos.

Adicionalmente se agrega un mensaje nuevo al protocolo de comunicación (Servidor -> Cliente), que es el mensaje de confirmación de recepción de un chunk de apuestas. Este mensaje se envia una vez que el servidor recibe exitosamente un chunk, para luego almacenarlo en el archivo `bets.csv`.  

Cabe aclarar que se sigue manteniendo el envío de un solo mensaje por conexión, pero se envian chunks de apuestas en cada mensaje. Idealmente se podría mantener la misma conexión para enviar todos los mensajes entre cliente y servidor, pero para el ejercicio actual esto causaría que los clientes solo pudieran conectarse de manera secuencial y tendrian que esperar a que otro cliente termine todos sus envios antes de poder comenzar con los suyos. Esto es solucionable añadiendo manejo concurrente de conexiones en el servidor, sin embargo este es el objetivo del ejercicio 8. 

Para realizar una verificación completa con 5 clientes, ejecutar:

- Iniciar la comunicación:

```bash
make docker-compose-up
```

- Observar los logs en una terminal adicional:

```bash
make docker-compose-logs
```

- Una vez se observe que los clientes han enviado todas las apuestas se puede verificar el tamaño final del archivo bets.csv (78697 lineas) que fue escrito dentro del contenedor del servidor. Esto se puede hacer con el siguiente comando:

```bash
docker exec server wc --lines bets.csv
```

## Ejercicio 7

Para este ejercicio se añade un identificador de tipo de mensaje como header para la comunicacion. Para esto, se extiede el mensaje básico de envio de chunks de apuestas definido en el ejercicio 5. **Se le añade el prefijo** `B`:

<p align="center">
<code>B{ ... }, ... , { ... };</code>
</p>

Adicionalmente se definen los siguientes nuevos tipos de mensajes:

- (Cliente -> Servidor) ***Mensaje de tipo notificacion*** (`N`) ***de finalización de envio de apuestas***. Este mensaje es enviado por el cliente para notificar al servidor que ha terminado de enviar todas las apuestas. Este mensaje solo contiene el identificador de la agencia que finaliza el envio. 

<p align="center">
<code>N{AgencyID:INT};</code>
</p>

- (Cliente -> Servidor) ***Mensaje de tipo consulta de resultados*** (`Q`). Este mensaje es enviado por el cliente para solicitar al servidor los resultados del sorteo. Este mensaje solo contiene el identificador de la agencia que solicita los resultados. 

<p align="center">
<code>Q{AgencyID:INT};</code>
</p>

- (Servidor -> Cliente) ***Mensaje de tipo respuesta de solicitud de resultados*** (`R`). Este mensaje es enviado por el servidor como respuesta positiva a la solicitud de resultados de una agencia. Este mensaje contiene los DNIs de los ganadores.

<p align="center">
<code>R{PlayerDocID:INT},{...},...,{...};</code>
</p>

- (Servidor -> Cliente) ***Mensaje de tipo espera tras solicitud de resultados*** (`W`). Este mensaje es enviado por el servidor como respuesta temporal a la solicitud de resultados de una agencia, indicandole que tiene que esperar ya que no se han realizado sorteos, dado que faltan agencias por notificarse que han terminado de enviar sus apuestas. Este mensaje solo contiene el identificador del tipo de mensaje.

<p align="center">
<code>W;</code>
</p>

Al finalizar el envio de apuestas se pueden observar los distintos mensajes añadidos en los logs de los clientes y el servidor.

Para verificar se puede volver a realizar el mismo caso de prueba que el ejercicio 6:

- Iniciar la comunicación:

```bash
make docker-compose-up
```

- Observar los logs:

```bash
make docker-compose-logs
```

- Tamaño final del archivo bets.csv (78697 lineas):

```bash
docker exec server wc --lines bets.csv
```

- Output adicional:

```bash
...
client2  | time="2024-03-25 23:07:54" level=info msg="action: results_phase_started | result: in_progress | client_id: 2"
client2  | time="2024-03-25 23:07:54" level=info msg="action: consulta_ganadores | result: success | client_id: 2 | cant_ganadores: 3 | dni_ganadores: [33791469 31660107 24813860]"
client2  | time="2024-03-25 23:07:54" level=info msg="action: loop_finished | result: success | client_id: 2"

...
server   | 2024-03-25 23:07:54 INFO     action: results_query_received | result: success | agency: 2
server   | 2024-03-25 23:07:54 INFO     action: accept_connections | result: in_progress
client5  | time="2024-03-25 23:07:58" level=info msg="action: results_phase_started | result: in_progress | client_id: 5"
server   | 2024-03-25 23:07:58 INFO     action: accept_connections | result: success | ip: 172.25.125.6
server   | 2024-03-25 23:07:58 INFO     action: results_query_received | result: success | agency: 5
server   | 2024-03-25 23:07:58 INFO     action: accept_connections | result: in_progress
client5  | time="2024-03-25 23:07:58" level=info msg="action: consulta_ganadores | result: success | client_id: 5 | cant_ganadores: 0 | dni_ganadores: []"
client5  | time="2024-03-25 23:07:58" level=info msg="action: loop_finished | result: success | client_id: 5"
client1  | time="2024-03-25 23:07:58" level=info msg="action: results_phase_started | result: in_progress | client_id: 1"
server   | 2024-03-25 23:07:58 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-03-25 23:07:58 INFO     action: results_query_received | result: success | agency: 1
client4  | time="2024-03-25 23:07:58" level=info msg="action: results_phase_started | result: in_progress | client_id: 4"
client3  | time="2024-03-25 23:07:58" level=info msg="action: results_phase_started | result: in_progress | client_id: 3"
client1  | time="2024-03-25 23:07:58" level=info msg="action: consulta_ganadores | result: success | client_id: 1 | cant_ganadores: 2 | dni_ganadores: [30876370 24807259]"
client1  | time="2024-03-25 23:07:58" level=info msg="action: loop_finished | result: success | client_id: 1"
server   | 2024-03-25 23:07:58 INFO     action: accept_connections | result: in_progress
server   | 2024-03-25 23:07:58 INFO     action: accept_connections | result: success | ip: 172.25.125.7
server   | 2024-03-25 23:07:58 INFO     action: results_query_received | result: success | agency: 4
client5 exited with code 0
client4  | time="2024-03-25 23:07:59" level=info msg="action: consulta_ganadores | result: success | client_id: 4 | cant_ganadores: 2 | dni_ganadores: [34963649 35635602]"
client4  | time="2024-03-25 23:07:59" level=info msg="action: loop_finished | result: success | client_id: 4"
server   | 2024-03-25 23:07:59 INFO     action: accept_connections | result: in_progress
server   | 2024-03-25 23:07:59 INFO     action: accept_connections | result: success | ip: 172.25.125.4
server   | 2024-03-25 23:07:59 INFO     action: results_query_received | result: success | agency: 3
client1 exited with code 0
client3  | time="2024-03-25 23:07:59" level=info msg="action: consulta_ganadores | result: success | client_id: 3 | cant_ganadores: 3 | dni_ganadores: [22737492 23328212 28188111]"
server   | 2024-03-25 23:07:59 INFO     action: accept_connections | result: in_progress
client3  | time="2024-03-25 23:07:59" level=info msg="action: loop_finished | result: success | client_id: 3"
client4 exited with code 0
client3 exited with code 0
```

## Ejercicio 8

Debido a la protección de objetos que tiene el GlobalInterpreterLock de Python, se decide utilizar la biblioteca `multiprocessing` para manejar conexiones concurrentes en el servidor en lugar de `threading`.

Ahora, los distintos clientes requieren de mecanismos de sincronización para:

- Lectura y escritura del archivo bets.csv por medio de las funciones brindadas por la catedra (no son process-safe).
- Acceso a la variable interna del servidor que almacena la cantidad de clientes que ya completaron el envio de apuestas, para poder decidir si se pueden enviar los resultados o no.

Para lo cual se utilizaran locks de la biblioteca `multiprocessing` para brindar exclusión mutua en las secciones críticas.
Segun la documentacion, estos locks difieren de los locks de `threading` en que de fondo utilizan IPC brindado por el sistema operativo, lo cual permite que sean utilizados por procesos distintos (como son multiples procesos no comparten memoria).

Como ahora se manejan conexiones concurrentes, las mismas pueden ser establecidas en simultaneo y de forma permanente durante toda la comunicación. Esto no se podía en los ejercicios anteriores que requerian establecer una conexion por cada uno o dos mensajes intercambiados, ya que de lo contrario los clientes debían esperar a que otro cliente terminara todo su proceso de comunicación antes de poder comenzar el suyo. Esto generaba un bloqueo ya que si un cliente terminaba con sus chunks, quedaría permanentemente esperando a los resultados dado que los otros clientes no pueden conectarse. Esto se soluciona paralelizando las conexiones. (Como nota auxiliar, se añadió un mensaje minimo de ack de confirmación para el mensaje notificacion de finalización de envio de apuestas. Anteriormente el servidor no respondia a este mensaje por la unicidad de conexion por mensaje, pero ahora al estar en un ambiente concurrente puede generar problemas con el formato de la notificación y las queries de resultados).

Para realizar una verificación nuevamente se puede realizar el mismo caso de prueba que los ultimos dos ejercicios:

```bash
make docker-compose-up
```

```bash
make docker-compose-logs
```

```bash
docker exec server wc --lines bets.csv
```

Output:

```bash
...
client1  | time="2024-03-26 01:56:13" level=info msg="action: chunk_ack_received | result: success | client_id: 1 | chunk_id: 539"
client1  | time="2024-03-26 01:56:13" level=info msg="action: notify_ack_received | result: success | client_id: 1"
client1  | time="2024-03-26 01:56:13" level=info msg="action: notify_phase_completed | result: success | client_id: 1"
client1  | time="2024-03-26 01:56:13" level=info msg="action: results_phase_started | result: in_progress | client_id: 1"
client1  | time="2024-03-26 01:56:13" level=info msg="action: consulta_ganadores | result: success | client_id: 1 | cant_ganadores: 2 | dni_ganadores: [30876370 24807259]"
client1  | time="2024-03-26 01:56:13" level=info msg="action: loop_finished | result: success | client_id: 1"
server   | 2024-03-26 01:56:12 INFO     action: chunk_received | result: success | agency: 3 | number_of_bets: 50
...
client5  | time="2024-03-26 01:56:17" level=info msg="action: results_phase_started | result: in_progress | client_id: 5"
server   | 2024-03-26 01:56:17 INFO     action: results_query_received | result: success | agency: 5
client3  | time="2024-03-26 01:56:17" level=info msg="action: results_phase_started | result: in_progress | client_id: 3"
server   | 2024-03-26 01:56:17 INFO     action: results_query_received | result: success | agency: 3
client4  | time="2024-03-26 01:56:17" level=info msg="action: results_phase_started | result: in_progress | client_id: 4"
server   | 2024-03-26 01:56:17 INFO     action: results_query_received | result: success | agency: 4
client5  | time="2024-03-26 01:56:18" level=info msg="action: consulta_ganadores | result: success | client_id: 5 | cant_ganadores: 0 | dni_ganadores: []"
client5  | time="2024-03-26 01:56:18" level=info msg="action: loop_finished | result: success | client_id: 5"
server   | 2024-03-26 01:56:18 DEBUG    action: receive_message | result: fail | error: connection_closed_by_client
client2  | time="2024-03-26 01:56:18" level=info msg="action: results_phase_started | result: in_progress | client_id: 2"
server   | 2024-03-26 01:56:18 INFO     action: results_query_received | result: success | agency: 2
client3  | time="2024-03-26 01:56:18" level=info msg="action: consulta_ganadores | result: success | client_id: 3 | cant_ganadores: 3 | dni_ganadores: [22737492 23328212 28188111]"
server   | 2024-03-26 01:56:18 DEBUG    action: receive_message | result: fail | error: connection_closed_by_client
client3  | time="2024-03-26 01:56:18" level=info msg="action: loop_finished | result: success | client_id: 3"
client5 exited with code 0
client4  | time="2024-03-26 01:56:18" level=info msg="action: consulta_ganadores | result: success | client_id: 4 | cant_ganadores: 2 | dni_ganadores: [34963649 35635602]"
client4  | time="2024-03-26 01:56:18" level=info msg="action: loop_finished | result: success | client_id: 4"
server   | 2024-03-26 01:56:18 DEBUG    action: receive_message | result: fail | error: connection_closed_by_client
client3 exited with code 0
client2  | time="2024-03-26 01:56:18" level=info msg="action: consulta_ganadores | result: success | client_id: 2 | cant_ganadores: 3 | dni_ganadores: [33791469 31660107 24813860]"
client2  | time="2024-03-26 01:56:18" level=info msg="action: loop_finished | result: success | client_id: 2"
server   | 2024-03-26 01:56:18 DEBUG    action: receive_message | result: fail | error: connection_closed_by_client
client4 exited with code 0
client2 exited with code 0
```
