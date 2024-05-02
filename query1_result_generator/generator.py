from shared.mq_connection_handler import MQConnectionHandler
import logging
import signal
from shared import constants

class Generator:
    def __init__(self, input_exchange_name: str, output_exchange_name: str, input_queue_name: str, output_queue_name: str):
        self.output_queue = output_queue_name
        self.results_msg = "[Q1 Results]:  (Title, Authors, Publisher, Publication Year)"
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]}, 
                                                         input_exchange_name=input_exchange_name, 
                                                         input_queues_to_recv_from=[input_queue_name])
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue_name, self.__get_results)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Q1 Result Generator")
        self.mq_connection_handler.close_connection()
        
    def start(self):
        self.mq_connection_handler.start_consuming()
        
    def __get_results(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            logging.info("Received EOF")
            self.mq_connection_handler.send_message(self.output_queue, self.results_msg)
            logging.info("Sending Q1 results to output queue")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.results_msg = "[Q1 Results]:"
        else: 
            self.results_msg += "\n" + msg 
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        