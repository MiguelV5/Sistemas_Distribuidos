from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
import csv
import io
from shared.monitorable_process import MonitorableProcess


TITLE_IDX = 0
AUTHORS_IDX = 1
PUBLISHER_IDX = 2
YEAR_IDX = 3


class FilterByTitle(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 title_keyword: str):
        super().__init__()
        self.output_queue = output_queue_name
        self.title_keyword = title_keyword
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue_name, self.__filter_books_by_title)
        
            
    def __filter_books_by_title(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            msg = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            row = next(msg)
            title = row[TITLE_IDX]
            authors = row[AUTHORS_IDX]
            publisher = row[PUBLISHER_IDX]
            year = row[YEAR_IDX]
            
            if self.title_keyword.lower() in title.lower():
                msg_to_send = f"{title},\"{authors}\",{publisher},{year}"           
                self.mq_connection_handler.send_message(self.output_queue, msg_to_send)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        
       
    def start(self):
        self.mq_connection_handler.channel.start_consuming()