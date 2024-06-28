# Error: falla de nodo en el sistema cuando el cliente tiene datos vacíos de libros

## Resumen

El nodo counter_of_decades_per_author no contemplaba el caso en el que se intentaba enviar los resultados sin datos al recibir un mensaje de tipo `EOF_B`, que para este controlador significa que debe enviar los resultados ya que no hay mas datos por recibir.

## Solución

Se corrigió el acceso al diccionario que no estaba previamente inicializado al no tener datos.

- Antes:

```python
def __send_results(self, client_id):
        ...
        authors_decades = list(self.state[client_id]["authors_decades"].items())
        ...
```

- Después:

```python
def __send_results(self, client_id):
        ...
        authors_decades = list(self.state[client_id].get("authors_decades", {}).items())
        ...
```
