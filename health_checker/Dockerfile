FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker && pip3 install pika

COPY health_checker /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]