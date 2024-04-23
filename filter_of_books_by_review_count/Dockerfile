FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY filter_of_books_by_review_count /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]