from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import logging
import csv
import io
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


TITLE_IDX = 0
AUTHORS_IDX = 1
SCORES_LIST_IDX = 2
DECADE_IDX = 3
REVIEW_COUNT_IDX = 4

class FilterByReviewsCount(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 output_exchange: str, 
                 input_queue: str, 
                 output_queue_towards_query3: str,
                 output_queue_towards_sorter: str, 
                 min_reviews: int, 
                 num_of_counters: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue_towards_query3 = output_queue_towards_query3
        self.output_queue_towards_sorter = output_queue_towards_sorter
        self.num_of_counters = int(num_of_counters)
        self.min_reviews = int(min_reviews)
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue_towards_query3: [self.output_queue_towards_query3], self.output_queue_towards_sorter: [output_queue_towards_sorter]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])        
        
    def start(self):
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue, self.state_handler_callback, self.__filter_books)
        self.mq_connection_handler.start_consuming()
        
    def __filter_books(self, body: SystemMessage):
        """ 
        The body is a csv line with the following format in the line: "title,review_score,decade,reviews_count" 
        The filter should filter out books with reviews_count less than min_reviews and send the result to the outputsqueue.
        """
        logging.debug(f"Received message from input queue: {body.payload}")
        if body.type == SystemMessageType.EOF_R:
            client_eofs_received = self.state.get(body.client_id, {}).get("eofs_received", 0) + 1
            self.state[body.client_id].update({"eofs_received": client_eofs_received})
            if client_eofs_received == self.num_of_counters:
                next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
                logging.info(f"Received EOF from all counters (for [ client_{body.client_id} ]). Sending EOF to output queues.")
                self.mq_connection_handler.send_message(self.output_queue_towards_query3, SystemMessage(SystemMessageType.EOF_R, body.client_id, self.controller_name, next_seq_num).encode_to_str())
                self.mq_connection_handler.send_message(self.output_queue_towards_sorter, SystemMessage(SystemMessageType.EOF_R, body.client_id, self.controller_name, next_seq_num).encode_to_str())
                self.update_self_seq_number(body.client_id, next_seq_num)
        elif body.type == SystemMessageType.ABORT:
            logging.info(f"[ABORT RECEIVED]: client: {body.client_id}")
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue_towards_query3, SystemMessage(SystemMessageType.ABORT, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            self.mq_connection_handler.send_message(self.output_queue_towards_sorter, SystemMessage(SystemMessageType.ABORT, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            self.state[body.client_id] = {}
        else:
            books = body.get_batch_iter_from_payload()
            payload_to_send_towards_query3 = ""
            payload_to_send_towards_sorter = ""
            for book in books:
                reviews_count = int(book[REVIEW_COUNT_IDX])
                if reviews_count >= self.min_reviews:
                    payload_to_send_towards_query3 += f"{book[TITLE_IDX]},{reviews_count},\"{book[AUTHORS_IDX]}\"\n"
                    payload_to_send_towards_sorter += f"{book[TITLE_IDX]},\"{book[SCORES_LIST_IDX]}\"\n"

            if payload_to_send_towards_query3 and payload_to_send_towards_sorter:
                next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)

                self.mq_connection_handler.send_message(self.output_queue_towards_query3, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, next_seq_num, payload_to_send_towards_query3).encode_to_str())
                self.update_self_seq_number(body.client_id, next_seq_num)
                self.mq_connection_handler.send_message(self.output_queue_towards_sorter, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, next_seq_num, payload_to_send_towards_sorter).encode_to_str())

                self.update_self_seq_number(body.client_id, next_seq_num)
