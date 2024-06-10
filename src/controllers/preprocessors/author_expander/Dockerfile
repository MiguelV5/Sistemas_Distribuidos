FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/preprocessors/author_expander /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]