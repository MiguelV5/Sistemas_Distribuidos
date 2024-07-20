# Correcciones recibidas:

- Ej. 1: Bien.

- Ej. 2: Bien.

- Ej. 3: Bien.

- Ej. 4: Regular. No se cierra el socket del servidor cuando se recibe la señal, lo que evita la finalización en caso de encontrarse operando con el mismo.

- Ej. 5: Bien.

- Ej. 6: Bien.

- Ej. 7: Bien.

- Ej. 8: Bien.

## Notas:

- \[**MAJOR**\] Ej4: el server socket no se cierra luego de recibir un sigterm. El flujo termina correctamente destrabando el accept pero el FD queda abierto. Si hubiera captado este error en la instancia de corrección inicial sería causa de reentrega. Por favor, revisar el código y no olvidarlo.

- Ej5: deja de controlar la variable de error al hacer writer.WriteString(msg)

- Ej5: handleDeliveryOfSingleChunk y handleDeliveryOfExceedingChunks son códigos copy-paste. Se puede refactorizar para hacer una única lógica. Nota: la estrategia de buscar el delimitador al final de cada paquete funciona correctamente solo porque el protocolo de la capa siguiente es request-reply. Se explicó en clase como realizar ciclos de recepción de 1024 bytes pero poder encontrar el delimitador en cualquier posición y bufferear el resto de los bytes recibidos para una implementación robusta.
