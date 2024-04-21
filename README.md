---

# [75.74] TP Distributed Books Analyzer - Sistemas Distribuidos - 1c2024

---

<br>
<p align="center">
  <img src="https://raw.githubusercontent.com/MiguelV5/MiguelV5/main/misc/logofiubatransparent_partialwhite.png" width="50%"/>
</p>
<br>

---

## Grupo 1

### Integrantes

| Nombre                                                              | Padrón |
| ------------------------------------------------------------------- | ------ |
| [Erick Martinez](https://github.com/erick12m)                       | 103745 |
| [Miguel Vasquez](https://github.com/MiguelV5)                       | 107378 |

---

## Introducción

Distributed Books Analyzer es un sistema distribuido de procesos que permiten realizar procesamiento de una extensa cantidad de datos, provenientes de [datasets de libros y reviews](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews) publicados en la plataforma de Amazon, con fines de análisis y aprovechamiento de multicomputing para la realización de consultas.

El sistema responde a las siguientes consultas:
- Título, autores y editoriales de los libros de categoría "Computers" entre 2000 y 2023 que contengan 'distributed' en su título
- Autores que tengan títulos publicados en al menos 10 décadas distintas
- Títulos y autores de los libros publicados en la década del 90' con al menos 500 calificaciones
- Top 10 libros con mejor promedio de calificación entre aquellos publicados en la década del 90’ con al menos 500 calificaciones
- Título de libros del genero "Ficción" cuyo sentimiento de reseña promedio esta en el percentil 90 más alto

Dicho conjunto de procesos se comunica mediante el uso de un middleware de mensajería, en este caso, RabbitMQ.

## Ejecución

Para ejecutar los procesos se emplea docker-compose, el cual se encarga de levantar todos los servicios necesarios para el correcto funcionamiento del sistema.

Para ello, se debe ejecutar el siguiente comando en la raíz del proyecto:

```bash
make docker-compose-up
```

Para detener la aplicación:
    
```bash
make docker-compose-down
```

## Informe

Para ver a detalle decisiones de diseño, arquitectura e implementación, referirse al [informe](https://github.com/erick12m/distributed-books-analyzer/blob/main/informe.pdf).


