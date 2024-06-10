FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/preprocessors/decade_preprocessor /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]