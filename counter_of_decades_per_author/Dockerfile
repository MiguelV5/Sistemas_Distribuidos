FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip3 install docker

COPY counter_of_decades_per_author /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]