FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY query5_result_generator /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]