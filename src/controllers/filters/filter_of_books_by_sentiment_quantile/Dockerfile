FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip3 install numpy

COPY src/controllers/filters/filter_of_books_by_sentiment_quantile /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]