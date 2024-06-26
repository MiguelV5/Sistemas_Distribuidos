import json
from shared.atomic_writer import AtomicWriter
from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared.protocol_messages import SystemMessage, SystemMessageType
from textblob import TextBlob
import math
from shared.monitorable_process import MonitorableProcess


from typing import TypeAlias

from shared.monitorable_process import ClientID_t
BookTitle_t: TypeAlias = str
AccumulatedPolarity_t: TypeAlias = float
TotalReviews_t: TypeAlias = int
BookData_t: TypeAlias = list[AccumulatedPolarity_t, TotalReviews_t]

POLARITY_IDX = 0
TOTAL_REVIEWS_IDX = 1


TITLE_IDX = 0
TEXT_IDX = 1

class SentimentAnalyzer(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 batch_size: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.batch_size = batch_size
        
        self.books_state_file_path = "books_state.json"
        self.books_state: dict[ClientID_t, dict[BookTitle_t, BookData_t]] = self.__load_books_state_file()

        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__handle_reviews_calculations)
        
    
    def start(self):
        self.mq_connection_handler.channel.start_consuming()

            
    def __handle_reviews_calculations(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            logging.info(f"Received EOF_R from [ client_{body.client_id} ]")
            self.__handle_eof_reviews(body.client_id)
        elif body.type == SystemMessageType.ABORT:
            logging.info(f"[ABORT RECEIVED]: client: {body.client_id}")
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.ABORT, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            self.state[body.client_id] = {}
        else:
            reviews = body.get_batch_iter_from_payload()
            for review in reviews:
                title = review[TITLE_IDX]
                text = review[TEXT_IDX]
                polarity = TextBlob(text).sentiment.polarity
                self.__add_polarity_for_book(body.client_id, title, polarity)
            self.__save_books_state_file()
            
        
    def __handle_eof_reviews(self, client_id):
        remaining_amount_of_books = len(self.books_state.get(client_id, {}))
        payload_current_size = 0
        payload_to_send = ""
        while (remaining_amount_of_books > 0) and (payload_current_size < self.batch_size):
            title, avg_polarity = self.__pop_average_polarity_of_book(client_id)
            payload_to_send += f"{title},{avg_polarity}" + "\n"
            payload_current_size += 1
            remaining_amount_of_books -= 1
            if payload_current_size == self.batch_size or remaining_amount_of_books == 0:
                seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
                self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
                self.update_self_seq_number(client_id, seq_num_to_send)
                payload_to_send = ""
                payload_current_size = 0

        seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
        self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.EOF_R, client_id, self.controller_name, seq_num_to_send).encode_to_str())
        self.update_self_seq_number(client_id, seq_num_to_send)
        logging.info("Sent EOF_R message to output queue")
        self.books_state[client_id] = {}
        self.__save_books_state_file()

    # ==============================================================================================================

        
    def __add_polarity_for_book(self, client_id: int, title: str, polarity: float):
        if client_id in self.books_state:
            if title in self.books_state[client_id]:
                self.books_state[client_id][title][POLARITY_IDX] = math.fsum(
                    [self.books_state[client_id][title][POLARITY_IDX], polarity]
                )
                self.books_state[client_id][title][TOTAL_REVIEWS_IDX] += 1
            else:
                self.books_state[client_id][title] = [polarity, 1]
        else:
            self.books_state[client_id] = {title: [polarity, 1]}



    def __pop_average_polarity_of_book(self, client_id) -> tuple[str, float]:
        """
        Returns the mean of the polarity of a single book and removes it from the accumulator
        """
        title, acc_values = self.books_state[client_id].popitem()

        polarity = acc_values[POLARITY_IDX]
        total_reviews = acc_values[TOTAL_REVIEWS_IDX]
        avg_polarity = polarity / total_reviews
        return title, avg_polarity
    
    
    # ==============================================================================================================


    def __save_books_state_file(self):
        writer = AtomicWriter(self.books_state_file_path)
        writer.write(json.dumps(self.books_state))
    

    def __load_books_state_file(self) -> dict:
        try:
            with open(self.books_state_file_path, 'r') as f:
                state_json = json.load(f)
                return {int(k): v for k, v in state_json.items()}
        except FileNotFoundError:
            return {}
        