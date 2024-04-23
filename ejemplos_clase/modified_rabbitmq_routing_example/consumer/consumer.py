#!/usr/bin/env python3
import pika
import time
import os

# Wait for rabbitmq to come up
# time.sleep(10)

consumer_id = os.environ["CONSUMER_ID"]
severities = os.environ["LOG_SEVERITY"].split(',')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

result = channel.queue_declare(queue=severities[0], durable=True)
queue_name = result.method.queue

for severity in severities:
    channel.queue_bind(
        exchange='direct_logs', queue=severity, routing_key=severity)

print('[{}] Waiting for messages. To exit press CTRL+C'.format(consumer_id))

def callback(ch, method, properties, body):
    print("[{}] Routing Key: {} - Message: {}".format(consumer_id, method.routing_key, body))

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()