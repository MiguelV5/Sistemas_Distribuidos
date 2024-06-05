FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip3 install docker

COPY year_preprocessor /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]