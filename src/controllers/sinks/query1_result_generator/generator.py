from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess

class Generator(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.results_msg = "[Q1 Results]:  (Title, Authors, Publisher, Publication Year)"
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]}, 
                                                         input_exchange_name=input_exchange_name, 
                                                         input_queues_to_recv_from=[input_queue_name])
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.__get_results)
        
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
        
        