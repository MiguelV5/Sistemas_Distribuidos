FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY filter_of_books_by_year /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]