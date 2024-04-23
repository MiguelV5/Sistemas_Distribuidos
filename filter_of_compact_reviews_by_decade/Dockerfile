FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY filter_of_compact_reviews_by_decade /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]