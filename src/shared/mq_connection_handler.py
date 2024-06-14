import functools
from typing import Callable, Optional
import pika

class MQConnectionHandler:
    def __init__(self, 
                 output_exchange_name: str | None, 
                 output_queues_to_bind: dict[str,list[str]] | None, 
                 input_exchange_name: str | None, 
                 input_queues_to_recv_from: list[str] | None,
                 aux_input_exchange_name: str | None = None
                 ):
        """
        Creates a connection to a RabbitMQ server and declares the necessary exchanges and queues.

        ## Important parameter details:
        - output_queues_to_bind: The dict keys are the names of the queues and the values are lists with the routing keys that each queue is interested on receiving. The exchange that the queues are bound to is the one defined in the output_exchange_name parameter. The binding is done by the producer to guarantee that the messages are not lost in case the consumer is not running.
        - input_queues_to_recv_from: The list values are the names of the queues that we want to consume from. The exchange that the queue is bound to is the one that is declared in the input_exchange_name parameter.
        
        Although parameters may be None, they can only be so in the rare case in which the handler is used solely for either sending only or receiving only. (Example of usage: server process)

        - aux_input_exchange_name: Rare usage, thus optional. Only used when the channel must consume from an additional exchange. To be used with proper message handling as it makes the start_consuming method to consume from the related queues of both exchanges. (Example of usage: merger process)
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=3600))
        self.channel = self.connection.channel()

        # Notation: 
        # A flow is defined as the combination of an exchange, a queue and their corresponding binding.
        if output_exchange_name is not None:
            self.__declare_output_flows(output_exchange_name, output_queues_to_bind)
        if input_exchange_name is not None:
            self.__declare_input_flows(input_exchange_name, input_queues_to_recv_from, aux_input_exchange_name)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.confirm_delivery()


    def __declare_input_flows(self, 
                              input_exchange_name: str, 
                              input_queues_to_recv_from: list[str],
                              aux_input_exchange_name: str | None = None
                              ):
        self.channel.exchange_declare(exchange=input_exchange_name, exchange_type='direct')
        if aux_input_exchange_name is not None:
            self.channel.exchange_declare(exchange=aux_input_exchange_name, exchange_type='direct')
        for queue_name in input_queues_to_recv_from:
            self.channel.queue_declare(queue=queue_name, durable=True)
    
        
    def __declare_output_flows(self, 
                               output_exchange_name: str, 
                               output_queues_to_bind: dict[str,list[str]]
                               ):
        # Note: Both the exchange and the queues are declared in the producer AND consumer since they will be created only once, but the bindings are only declared in the producer as per mentioned earlier.
        self.output_exchange_name = output_exchange_name
        self.channel.exchange_declare(exchange=output_exchange_name, exchange_type='direct')
        for queue_name, binding_keys in output_queues_to_bind.items():
            self.channel.queue_declare(queue=queue_name, durable=True)
            for binding_key in binding_keys:
                self.channel.queue_bind(exchange=output_exchange_name, queue=queue_name, routing_key=binding_key)


    def setup_callbacks_for_input_queue(self, 
                                        queue_name: str, 
                                        state_handler_callback: Callable, 
                                        inner_processor: Optional[Callable] = None):
        if inner_processor is None:
            self.channel.basic_consume(queue=queue_name, on_message_callback=state_handler_callback)
        else:
            self.channel.basic_consume(queue=queue_name, on_message_callback=functools.partial(state_handler_callback, args=(inner_processor)))

    def start_consuming(self):
        self.channel.start_consuming()


    def send_message(self, routing_key, msg_body):
        """
        Sends a message with a specified routing_key to inform the output_exchange about which queues to route the message to.
        """
        self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=routing_key, body=msg_body, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent), mandatory=True)


    def close_connection(self):
        self.channel.stop_consuming()
        self.connection.close()

