FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip3 install docker

COPY filter_of_merged_reviews_by_book_genre /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]