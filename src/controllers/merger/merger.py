from shared.mq_connection_handler import MQConnectionHandler
import logging
import io
import csv
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


BOOK_TITLE_IDX = 0
BOOK_AUTHORS_IDX = 1
BOOK_CATEGORIES_IDX = 2
BOOK_DECADE_IDX = 3


REVIEW_TITLE_IDX = 0
REVIEW_SCORE_IDX = 1
REVIEW_TEXT_IDX = 2

class Merger(MonitorableProcess):
    def __init__(self, input_exchange_name_reviews: str,
                 input_exchange_name_books: str,
                 output_exchange_name: str,
                 input_queue_of_reviews: str,
                 input_queue_of_books: str,
                 output_queue_of_compact_reviews: str,
                 output_queue_of_full_reviews: str,
                 output_queue_of_books_confirms: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange_name_reviews = input_exchange_name_reviews
        self.input_exchange_name_books = input_exchange_name_books
        self.output_exchange_name = output_exchange_name
        self.input_queue_of_reviews = input_queue_of_reviews
        self.input_queue_of_books = input_queue_of_books
        self.output_queue_of_compact_reviews = output_queue_of_compact_reviews
        self.output_queue_of_full_reviews = output_queue_of_full_reviews
        self.output_queue_of_books_confirms = output_queue_of_books_confirms
        self.mq_connection_handler = None
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name,
                                                         output_queues_to_bind={
                                                             self.output_queue_of_compact_reviews: [self.output_queue_of_compact_reviews],
                                                             self.output_queue_of_full_reviews: [self.output_queue_of_full_reviews],
                                                             self.output_queue_of_books_confirms: [self.output_queue_of_books_confirms]},
                                                         input_exchange_name=self.input_exchange_name_reviews,
                                                         input_queues_to_recv_from=[self.input_queue_of_reviews, self.input_queue_of_books],
                                                         aux_input_exchange_name=self.input_exchange_name_books)
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_books, self.state_handler_callback, self.__handle_books_preprocessors_msgs) 
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_reviews, self.state_handler_callback, self.__handle_review_preprocessor_msgs)
        self.mq_connection_handler.channel.start_consuming()

            
    def __handle_books_preprocessors_msgs(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            self.__handdle_eof_books(body.client_id)
        else:
            self.__handle_incoming_books_data(body)

    def __handle_incoming_books_data(self, body: SystemMessage):
        books_batch = body.get_batch_iter_from_payload()
        for book in books_batch:
            if book: 
                title = book[BOOK_TITLE_IDX]
                self.__update_books_data_state(body.client_id, title, book)

    def __handdle_eof_books(self, client_id):
        logging.info("Received EOF_B. Saving and sending confirmation to server.")
        # Needs to force state saving before sending confirmation. Otherwise the last book might be lost.
        self.save_state_file()
        msg_for_server = SystemMessage(SystemMessageType.EOF_B, client_id, self.controller_name, 1).encode_to_str()
        self.mq_connection_handler.send_message(self.output_queue_of_books_confirms, msg_for_server)
        logging.info("Sent EOF_B confirmation to server")

    
        
    def __handle_review_preprocessor_msgs(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            self.__handle_eof_reviews(body.client_id)
        else:
            self.__handle_incoming_reviews_data(body)

    def __handle_eof_reviews(self, client_id):
        seq_num_to_send = self.get_next_seq_number(client_id, self.controller_name)
        msg_to_send = SystemMessage(SystemMessageType.EOF_R, client_id, self.controller_name, seq_num_to_send).encode_to_str()
        # self.mq_connection_handler.send_message(self.output_queue_of_compact_reviews, msg_to_send)
        logging.info("Sent EOF_R message to compact reviews queue")
        msg_to_send = SystemMessage(SystemMessageType.EOF_R, client_id, self.controller_name, seq_num_to_send).encode_to_str()
        # self.mq_connection_handler.send_message(self.output_queue_of_full_reviews, msg_to_send)
        logging.info("Sent EOF_R message to full reviews queue")
        self.update_self_seq_number(client_id, seq_num_to_send)
        self.__update_books_data_state(client_id, None, None, reset_for_client=True)

    def __handle_incoming_reviews_data(self, body: SystemMessage):
        compact_output_payload = ""
        full_output_payload = ""
        reviews_batch = body.get_batch_iter_from_payload()
        for review in reviews_batch:
            title = review[REVIEW_TITLE_IDX]
            score = review[REVIEW_SCORE_IDX]
            text = review[REVIEW_TEXT_IDX]
            books_data_of_client = self.state.get(body.client_id, {}).get("books_data", {})
            if title in books_data_of_client:
                compact_review, full_review = self.__merge_review_with_book(title, score, text, books_data_of_client[title])
                compact_output_payload += compact_review 
                full_output_payload += full_review 
        if compact_output_payload:
            seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
            msg_to_send = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, compact_output_payload).encode_to_str()
            # self.mq_connection_handler.send_message(self.output_queue_of_compact_reviews, msg_to_send)
            self.update_self_seq_number(body.client_id, seq_num_to_send)
        if full_output_payload:
            seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
            msg_to_send = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, full_output_payload).encode_to_str()
            # self.mq_connection_handler.send_message(self.output_queue_of_full_reviews, msg_to_send)
            self.update_self_seq_number(body.client_id, seq_num_to_send)

    def __merge_review_with_book(self, title, score, text, book_data):
        authors = book_data[BOOK_AUTHORS_IDX]
        categories = book_data[BOOK_CATEGORIES_IDX]
        decade = book_data[BOOK_DECADE_IDX]
        compact_review = f"{title},\"{authors}\",{score},{decade}" + "\n"
        full_review = f"{title},\"{categories}\",{text}" + "\n"
        return compact_review, full_review
    

        
    def __update_books_data_state(self, client_id, title, book_data, reset_for_client=False):
        if reset_for_client:
            self.state[client_id]["books_data"] = {}
        if client_id in self.state:
            if "books_data" in self.state[client_id]:
                self.state[client_id]["books_data"][title] = book_data
            else:
                self.state[client_id]["books_data"] = {title: book_data}
        else:
            self.state[client_id] = {"books_data": {title: book_data}}
            
            
                    
                    

        
    