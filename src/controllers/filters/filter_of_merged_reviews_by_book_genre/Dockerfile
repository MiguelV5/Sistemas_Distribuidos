FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/filters/filter_of_merged_reviews_by_book_genre /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]