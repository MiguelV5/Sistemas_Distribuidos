#!/usr/bin/env python3
import pika
import os
import random
import time

# Wait for rabbitmq to come up
# time.sleep(10)

exchange_name = os.environ["EXCHANGE_NAME"]
severity = "info"
queue_name = severity + "_" + exchange_name

# Create RabbitMQ communication channel
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

channel.queue_declare(queue=queue_name, durable=True)
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

for i in range(50):
    message = "[{}]".format(severity)
    channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=message)
    print(" [x] Sent %r" % message)
    time.sleep(0.05)

connection.close()
