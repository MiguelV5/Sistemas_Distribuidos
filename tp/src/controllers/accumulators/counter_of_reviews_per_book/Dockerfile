FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/accumulators/counter_of_reviews_per_book /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]