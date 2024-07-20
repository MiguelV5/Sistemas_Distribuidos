FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/sinks/query3_result_generator /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]