from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
import csv
import io
from shared.monitorable_process import MonitorableProcess


TITLE_IDX = 0
CATEGORIES_IDX = 1
TEXT_IDX = 2

class FilterReviewByBookGenre(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 genre_to_filter: str,
                 num_of_input_workers: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.genre_to_filter = genre_to_filter
        self.eofs_received = 0
        self.num_of_input_workers = num_of_input_workers
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue_name, self.__filter_reviews_by_book_genre)
        
            
    def __filter_reviews_by_book_genre(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.eofs_received += 1
            if self.eofs_received == self.num_of_input_workers:
                logging.info("All EOFs received. Sending to output queue")
                self.mq_connection_handler.send_message(self.output_queue, constants.FINISH_MSG)
                self.eofs_received = 0
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            batch = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batch_to_send = ""
            for row in batch:
                title = row[TITLE_IDX]
                categories = row[CATEGORIES_IDX]
                text = row[TEXT_IDX]
                if self.genre_to_filter in categories.lower():
                    batch_to_send += f"{title},{text}" + "\n"
            if batch_to_send:
                self.mq_connection_handler.send_message(self.output_queue, batch_to_send)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        
       
    def start(self):
        self.mq_connection_handler.channel.start_consuming()