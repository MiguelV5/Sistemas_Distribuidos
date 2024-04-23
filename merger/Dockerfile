FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY merger /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]