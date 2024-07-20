from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
import csv
import io
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


TITLE_IDX = 0
AUTHORS_IDX = 1
PUBLISHER_IDX = 2
YEAR_IDX = 3
CATEGORIES_IDX = 4


class FilterByGenreAndYear(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 min_year_to_filter: int, 
                 max_year_to_filter: int,
                 genre_to_filter: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.min_year_to_filter = int(min_year_to_filter)
        self.max_year_to_filter = int(max_year_to_filter)
        self.genre_to_filter = genre_to_filter
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__filter_books_by_year_and_genre)
        
            
    def __filter_books_by_year_and_genre(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
        elif body.type == SystemMessageType.ABORT:
            logging.info(f"[ABORT RECEIVED]: client: {body.client_id}")
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.ABORT, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            self.state[body.client_id] = {}       
        else:
            books = body.get_batch_iter_from_payload()
            payload_to_send = ""
            for book in books:
                title = book[TITLE_IDX]
                authors = book[AUTHORS_IDX]
                publisher = book[PUBLISHER_IDX]
                year_str = book[YEAR_IDX]
                categories = book[CATEGORIES_IDX]
                if int(year_str) >= self.min_year_to_filter and \
                        int(year_str) <= self.max_year_to_filter and \
                        self.genre_to_filter in categories:
                    payload_to_send += f"{title},\"{authors}\",{publisher},{year_str}" + "\n"    
            if payload_to_send:
                seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
                self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
                self.update_self_seq_number(body.client_id, seq_num_to_send)

       
    def start(self):
        self.mq_connection_handler.channel.start_consuming()