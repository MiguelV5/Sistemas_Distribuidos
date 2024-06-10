FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/filters/filter_of_authors_by_decade /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]