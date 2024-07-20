FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/filters/sorter_of_books_by_score_average /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]