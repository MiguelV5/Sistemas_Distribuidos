import io
from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


TITLE_IDX = 0
AUTHORS_IDX = 1
YEAR_IDX = 2
CATEGORIES_IDX = 3
REQUIRED_SIZE_OF_ROW = 4

class DecadePreprocessor(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 input_queue: str, 
                 output_exchange: str, 
                 output_queue_towards_expander: str, 
                 output_queues_towards_mergers: list[str], 
                 controller_name: str):
        super().__init__(controller_name)

        self.output_queue_towards_expander = output_queue_towards_expander
        self.output_queues_towards_mergers = output_queues_towards_mergers
        
        output_queues_to_bind = {output_queue_towards_expander: [output_queue_towards_expander]}
        for output_queue_towards_merger in output_queues_towards_mergers:
            output_queues_to_bind[output_queue_towards_merger] = [output_queue_towards_merger]

        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         output_queues_to_bind,
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue, self.state_handler_callback, self.__process_msg_from_prev_preprocessor)


    def __process_msg_from_prev_preprocessor(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            self.__handle_eof(body)
        else:
            self.__apply_preprocessing_to_batch_and_send(body)

    
    def __handle_eof(self, body: SystemMessage):
        logging.info(f"Received EOF_B from client: {body.client_id}")
        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        msg_to_send = SystemMessage(SystemMessageType.EOF_R, body.client_id, self.controller_name, seq_num_to_send).encode_to_str()
        for output_queue in self.output_queues_towards_mergers:
            self.mq_connection_handler.send_message(output_queue, msg_to_send)
        self.mq_connection_handler.send_message(self.output_queue_towards_expander, msg_to_send)
        self.update_self_seq_number(body.client_id, seq_num_to_send)


    def __apply_preprocessing_to_batch_and_send(self, body: SystemMessage):
        books_batch = body.get_batch_iter_from_payload()
        payload_to_send_towards_expander = ""
        payload_to_send_towards_mergers = {output_queue: "" for output_queue in self.output_queues_towards_mergers}
        for book in books_batch:
            if len(book) != REQUIRED_SIZE_OF_ROW:
                continue
            title = book[TITLE_IDX]
            authors = book[AUTHORS_IDX]
            year = book[YEAR_IDX]
            categories = book[CATEGORIES_IDX]
            decade = self.__extract_decade(year)

            payload_to_send_towards_expander += self.__format_authors_for_expander(authors, decade)
            selected_merger_queue = self.__select_merger_queue(title)
            payload_to_send_towards_mergers[selected_merger_queue] += self.__format_book_for_merger(title, authors, categories, decade)
            
        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        if payload_to_send_towards_expander:
            msg_for_expander = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send_towards_expander).encode_to_str()
            # self.mq_connection_handler.send_message(self.output_queue_towards_expander, msg_for_expander)
            
        for output_queue in self.output_queues_towards_mergers:
            if payload_to_send_towards_mergers[output_queue]:
                msg_for_merger = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send_towards_mergers[output_queue]).encode_to_str()
                # self.mq_connection_handler.send_message(output_queue, msg_for_merger)
        self.update_self_seq_number(body.client_id, seq_num_to_send)
        


    def __format_authors_for_expander(self, authors: str, decade: int) -> str:
        return f"\"{authors}\",{decade}" + "\n"

    def __format_book_for_merger(self, title: str, authors: str, categories: str, decade: int) -> str:
        return f"{title},\"{authors}\",\"{categories}\",{decade}" + "\n"

    def __extract_decade(self, year: str) -> int:
        year = int(year)
        decade = year - (year % 10)
        return decade

    def __select_merger_queue(self, title: str) -> str:
        """
        Should return the queue name where the review should be sent to.
        It uses the hash of the title to select a queue on self.output_queue_towards_mergers
        """
        hash_value = hash(title)
        queue_index = hash_value % len(self.output_queues_towards_mergers)
        return self.output_queues_towards_mergers[queue_index]
    

    def start(self):
        self.mq_connection_handler.start_consuming()
