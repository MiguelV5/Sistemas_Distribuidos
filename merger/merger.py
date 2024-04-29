from shared.mq_connection_handler import MQConnectionHandler
import logging
import io
import csv
from shared import constants


BOOK_TITLE_IDX = 0
BOOK_AUTHORS_IDX = 1
BOOK_CATEGORIES_IDX = 2
BOOK_DECADE_IDX = 3

REVIEW_TITLE_IDX = 0
REVIEW_SCORE_IDX = 1
REVIEW_TEXT_IDX = 2
class Merger:
    def __init__(self, input_exchange_name_reviews: str,
                 input_exchange_name_books: str,
                 output_exchange_name: str,
                 input_queue_name_reviews: str,
                 input_queue_name_books: str,
                 output_queue_name_compact_reviews: str,
                 output_queue_name_full_reviews: str):
        self.input_exchange_name_reviews = input_exchange_name_reviews
        self.input_exchange_name_books = input_exchange_name_books
        self.output_exchange_name = output_exchange_name
        self.input_queue_name_reviews = input_queue_name_reviews
        self.input_queue_name_books = input_queue_name_books
        self.output_queue_name_compact_reviews = output_queue_name_compact_reviews
        self.output_queue_name_full_reviews = output_queue_name_full_reviews
        self.books_finished = False
        self.mq_connection_handler = None
        self.book_data = {}
        self.reviews_buffer = []
        
    def start(self):
        try:
            self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name,
                                                             output_queues_to_bind={
                                                                 self.output_queue_name_compact_reviews: [self.output_queue_name_compact_reviews],
                                                                 self.output_queue_name_full_reviews: [self.output_queue_name_full_reviews]},
                                                             input_exchange_name=self.input_exchange_name_reviews,
                                                             input_queues_to_recv_from=[self.input_queue_name_reviews, self.input_queue_name_books],
                                                             aux_input_exchange_name=self.input_exchange_name_books)
        except Exception as e:
            logging.error(f"Error while creating the MQConnectionHandler object: {e}")
            
        #try:
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name_books, self.__handle_books) 
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name_reviews, self.__handle_reviews)
        self.mq_connection_handler.channel.start_consuming()
        #except Exception as e:
            #logging.error(f"Error while setting up the callbacks: {e}")
            
    def __handle_books(self, ch, method, properties, body):
        """
        The message should have the following format: title,authors,categories,decade
        """
        msg = body.decode()
        logging.info(f"Received message from books queue: {msg}")
        if msg == constants.FINISH_MSG:
            self.books_finished = True
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        batch = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
        for line in batch:
            if line: 
                title = line[BOOK_TITLE_IDX]
                self.book_data[title] = line
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __handle_reviews(self, ch, method, properties, body):
        """
        The message should have the following format: title,review/score,review/text
        """
        msg = body.decode() 
        logging.info(f"Received message from reviews queue: {msg}")

        if msg == constants.FINISH_MSG:
            self.__handle_eof_reviews()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        output_msg_compact = ""
        output_msg_full = ""
        for line in msg.split("\n"):
            if line:
                title, score, text = line.split(",")
                if title in self.book_data:
                    compact, full = self.__generate_line_output(title, score, text)
                    output_msg_compact += compact + "\n"
                    output_msg_full += full + "\n"
                elif not self.books_finished:
                    self.reviews_buffer.append(line)
                # If the title of the reviews is not in the book_data after we got all books, it is discarded
        if output_msg_compact:  
            self.mq_connection_handler.send_message(self.output_queue_name_compact_reviews, output_msg_compact)
            logging.info(f"Sent message to compact reviews queue: {output_msg_compact}")
        if output_msg_full:
            self.mq_connection_handler.send_message(self.output_queue_name_full_reviews, output_msg_full)
            logging.info(f"Sent message to full reviews queue: {output_msg_full}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __generate_line_output(self, title, score, text):
        book_data = self.book_data.get(title)
        authors = book_data[BOOK_AUTHORS_IDX]
        categories = book_data[BOOK_CATEGORIES_IDX]
        decade = book_data[BOOK_DECADE_IDX]
        compact_review = f"{title},\"{authors}\",{score},{decade}"
        full_review = f"{title},\"{categories}\",{text}"
        return compact_review, full_review
        
    def __handle_eof_reviews(self):
        output_msg_compact = ""
        output_msg_full = ""
        for line in self.reviews_buffer:
            title, score, text = line.split(",")
            if title in self.book_data:
                compact, full = self.__generate_line_output(title, score, text)
                output_msg_compact += compact + "\n"
                output_msg_full += full + "\n"
        self.reviews_buffer = []
        if output_msg_compact:
            self.mq_connection_handler.send_message(self.output_queue_name_compact_reviews, output_msg_compact)
            logging.info(f"Sent message to compact reviews queue: {output_msg_compact}")
        if output_msg_full:
            self.mq_connection_handler.send_message(self.output_queue_name_full_reviews, output_msg_full)
            logging.info(f"Sent message to full reviews queue: {output_msg_full}")
        self.mq_connection_handler.send_message(self.output_queue_name_compact_reviews, constants.FINISH_MSG)
        logging.info(f"Sent EOF message to compact reviews queue")
        self.mq_connection_handler.send_message(self.output_queue_name_full_reviews, constants.FINISH_MSG)
        logging.info(f"Sent EOF message to full reviews queue")
                    
        

            
            
                    
                    

        
    