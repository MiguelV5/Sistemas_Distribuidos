from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess

TITLE_IDX = 0
AVG_POLARITY_IDX = 1

class Generator(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.resulting_books_batch = constants.PAYLOAD_HEADER_Q5
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.__accumulate_results)
        
            
    def __accumulate_results(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.__handle_eof_reviews()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self.resulting_books_batch += msg + "\n"
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __handle_eof_reviews(self):
        logging.info("Sending Q5 results to output queue")
        self.mq_connection_handler.send_message(self.output_queue, self.resulting_books_batch)
        self.resulting_books_batch = "[Q5 Results]:"


    def start(self):
        self.mq_connection_handler.channel.start_consuming()

