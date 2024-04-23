FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY counter_of_reviews_per_book /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]