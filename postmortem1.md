# Error: datos incorrectamente guardados del lado del cliente

## Resumen

El cliente utilizaba `lstrip` en lugar de `removeprefix` a la hora de guardar resultados que le llegaban del servidor. Esto provocaba que los datos no se guardaran correctamente y hacía parecer que el sistema estaba fallando en devolver resultados consistentes.

## Solución

Se cambió el uso al metodo correcto:

```python
def __save_results_to_file(self, results_payload: str): 
        file_path = self.results_dir_path
        for i, payload_header in enumerate(constants.PAYLOAD_HEADERS):
            if results_payload.startswith(payload_header):
                file_path += f"query{i+1}.csv"
                results_payload = results_payload.removeprefix(payload_header)
                break
        
        with open(file_path, 'a') as file:
            file.write(results_payload)
```
