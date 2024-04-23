FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY consumer.py /root/consumer.py
ENTRYPOINT ["/root/consumer.py"]