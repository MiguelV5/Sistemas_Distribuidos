FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/accumulators/counter_of_decades_per_author /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]