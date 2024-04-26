FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY book_sanitizer /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]
