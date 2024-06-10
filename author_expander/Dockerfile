FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY author_expander /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]