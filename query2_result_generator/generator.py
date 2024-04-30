from shared.mq_connection_handler import MQConnectionHandler
import logging
import signal
from shared import constants


class Generator:
    def __init__(self, input_exchange_name: str, output_exchange_name: str, input_queue_name: str, output_queue_name: str, filters_quantity: int):
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.response_msg = "Q2 Results: "
        self.eofs_received = 0
        self.filters_quantity = filters_quantity
        self.mq_connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Q2 Result Generator")
        self.mq_connection_handler.stop_consuming()
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]}, 
                                                         input_exchange_name=self.input_exchange_name, 
                                                         input_queues_to_recv_from=[self.input_queue_name])
        logging.info(f"MQConnectionHandler created successfully. Parameters: {self.input_exchange_name}, {self.output_exchange_name}, {self.input_queue_name}, {self.output_queue_name}")
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name, self.__get_results)
        self.mq_connection_handler.start_consuming()
        
    def __get_results(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.eofs_received += 1
            if int(self.eofs_received) == int(self.filters_quantity):
                logging.info("Received all EOFs")
                self.mq_connection_handler.send_message(self.output_queue_name, self.response_msg)
                logging.info(f"Sent response message to output queue: {self.response_msg}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.mq_connection_handler.close_connection()
            else: 
                ch.basic_ack(delivery_tag=method.delivery_tag)
        else: 
            self.response_msg += '\n' + msg 
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        