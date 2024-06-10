FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY review_sanitizer /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]