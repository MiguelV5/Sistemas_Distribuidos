# Error: datos incorrectamente guardados del lado del cliente

## Resumen

El cliente utilizaba `lstrip` en lugar de `removeprefix` a la hora de guardar resultados que le llegaban del servidor. Esto provocaba que los datos no se guardaran correctamente y hacía parecer que el sistema estaba fallando en devolver resultados consistentes.

## Solución

Se cambió el uso al metodo correcto. La diferencia radica en que `lstrip` elimina todos los caracteres que coincidan con los que se le pasan como argumento, mientras que `removeprefix` elimina solo el prefijo que se le pasa como argumento.

Según la documentación oficial de Python:
 
> str.lstrip([chars]): Return a copy of the string with leading characters removed. The chars argument is a string specifying the set of characters to be removed ... The chars argument is not a prefix; rather, all combinations of its values are stripped

- Antes:

```python
def __save_results_to_file(self, results_payload: str): 
        ...
        for i, payload_header in enumerate(constants.PAYLOAD_HEADERS):
            if results_payload.startswith(payload_header):
                ...
                results_payload = results_payload.lstrip(payload_header)
                ...
        ...
```

- Después:

```python
def __save_results_to_file(self, results_payload: str): 
        ...
        for i, payload_header in enumerate(constants.PAYLOAD_HEADERS):
            if results_payload.startswith(payload_header):
                ...
                results_payload = results_payload.removeprefix(payload_header)
                ...
        ...
```
