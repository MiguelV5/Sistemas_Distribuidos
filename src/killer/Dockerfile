FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker && pip3 install pika

COPY src/killer /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]