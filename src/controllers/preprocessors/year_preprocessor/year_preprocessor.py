import io
from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
from shared import constants
import re
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

TITLE_IDX = 0
AUTHORS_IDX = 1
PUBLISHER_IDX = 2
PUBLISHED_DATE_IDX = 3
CATEGORIES_IDX = 4
REQUIRED_SIZE_OF_ROW = 5


class YearPreprocessor(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 input_queue: str, 
                 output_exchange: str, 
                 output_queue_towards_preproc: str, 
                 output_queue_towards_filter: str,
                 controller_name: str):
        super().__init__(controller_name)

        self.output_queue_towards_preproc = output_queue_towards_preproc
        self.output_queue_towards_filter = output_queue_towards_filter
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue_towards_preproc: [output_queue_towards_preproc],
                                                          output_queue_towards_filter: [output_queue_towards_filter]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue, self.state_handler_callback, self.__process_msg_from_sanitizer)

    def __process_msg_from_sanitizer(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            self.__handle_eof(body)
        else:
            self.__apply_preprocessing_to_batch_and_send(body)


    def __handle_eof(self, body: SystemMessage):
        logging.info(f"Received EOF_B from client: {body.client_id}")
        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        msg_to_send = SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str()
        self.mq_connection_handler.send_message(self.output_queue_towards_preproc, msg_to_send)
        self.mq_connection_handler.send_message(self.output_queue_towards_filter, msg_to_send)
        self.update_self_seq_number(body.client_id, seq_num_to_send)


    def __apply_preprocessing_to_batch_and_send(self, body: SystemMessage):
        books_batch = body.get_batch_iter_from_payload()
        payload_to_send_towards_preproc = ""
        payload_to_send_towards_filter = ""
        for book in books_batch:
            if len(book) != REQUIRED_SIZE_OF_ROW:
                continue
            title = book[TITLE_IDX]
            authors = book[AUTHORS_IDX]
            publisher = book[PUBLISHER_IDX]
            published_date = book[PUBLISHED_DATE_IDX]
            categories = book[CATEGORIES_IDX]
            year = self.__extract_year(published_date)
            if year is None:
                continue

            payload_to_send_towards_preproc += self.__format_book_for_preproc(title, authors, year, categories)
            payload_to_send_towards_filter += self.__format_book_for_filter(title, authors, publisher, year, categories)
        
        if payload_to_send_towards_preproc and payload_to_send_towards_filter:
            seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
            msg_for_preproc = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send_towards_preproc).encode_to_str()
            msg_for_filter = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send_towards_filter).encode_to_str()
            self.mq_connection_handler.send_message(self.output_queue_towards_preproc, msg_for_preproc)
            self.mq_connection_handler.send_message(self.output_queue_towards_filter, msg_for_filter)
            self.update_self_seq_number(body.client_id, seq_num_to_send)

    def __format_book_for_preproc(self, title, authors, year, categories):
        return f"{title},\"{authors}\",{year},\"{categories}\"" + "\n"
    
    def __format_book_for_filter(self, title, authors, publisher, year, categories):
        return f"{title},\"{authors}\",{publisher},{year},\"{categories}\"" + "\n"


    def __extract_year(self, date):        
        if date:
            year_regex = re.compile('[^\d]*(\d{4})[^\d]*')
            result = year_regex.search(date)
            return int(result.group(1)) if result else None
        return None

                

                
                




    def start(self):
        self.mq_connection_handler.start_consuming()
