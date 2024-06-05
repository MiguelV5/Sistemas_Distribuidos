FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip3 install docker && pip3 install numpy

COPY filter_of_books_by_sentiment_quantile /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]