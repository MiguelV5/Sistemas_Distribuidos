#!/usr/bin/env python3
import pika
import time
import os

# Wait for rabbitmq to come up
# time.sleep(10)

consumer_id = os.environ["CONSUMER_ID"]
exchange_name1 = os.environ["EXCHANGE_NAME1"]
exchange_name2 = os.environ["EXCHANGE_NAME2"]
queue_name1 = os.environ["LOG_SEVERITY1"] + "_" + exchange_name1
queue_name2 = os.environ["LOG_SEVERITY2"] + "_" + exchange_name2
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()


channel.exchange_declare(exchange=exchange_name1, exchange_type='direct')
channel.queue_declare(queue=queue_name1, durable=True)


channel.exchange_declare(exchange=exchange_name2, exchange_type='direct')
channel.queue_declare(queue=queue_name2, durable=True)


print('[{}] Waiting for messages. To exit press CTRL+C'.format(consumer_id))
def callback(ch, method, properties, body):
    print("[{}] Routing Key: {} - Message: {}".format(consumer_id, method.routing_key, body))
    time.sleep(0.1)
    print("[{}] Done".format(consumer_id))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue=queue_name1, on_message_callback=callback)

channel.basic_consume(
    queue=queue_name2, on_message_callback=callback)

channel.start_consuming()