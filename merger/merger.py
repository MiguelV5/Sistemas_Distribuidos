from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
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
                                                             output_queues_to_bind={self.output_queue_name_compact_reviews: [self.output_queue_name_compact_reviews],
                                                                                   self.output_queue_name_full_reviews: [self.output_queue_name_full_reviews]},
                                                             input_exchange_name=self.input_exchange_name_reviews,
                                                             input_queues_to_recv_from=[self.input_queue_name_reviews, self.input_queue_name_books],
                                                             aux_input_exchange_name=self.input_exchange_name_books)
        except Exception as e:
            logging.error(f"Error while creating the MQConnectionHandler object: {e}")
            
        try:
            self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name_books, self.__handle_books) 
            self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name_reviews, self.__handle_reviews)
            self.mq_connection_handler.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error while setting up the callbacks: {e}")
            
    def __handle_books(self, ch, method, properties, body):
        """
        The message should have the following format: title,authors,categories,decade
        """
        msg = body.decode()
        if msg == "EOF":
            self.books_finished = True
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        for line in msg.split("\n"):
            if line: 
                title = line.split(",")[0]
                self.book_data[title] = line
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __handle_reviews(self, ch, method, properties, body):
        """
        The message should have the following format: title,review/score,review/text
        """
        msg = body.decode() 
        if msg == "EOF":
            self.__handle_eof_reviews()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        for line in msg.split("\n"):
            if line:
                title, score, text = line.split(",")
                if not self.books_finished and title not in self.book_data:
                    self.reviews_buffer.append(line)
                elif title in self.book_data:
                    self.__send_output_msgs(title, score, text)
                # If the title of the reviews is not in the book_data after we got all books, it is discarded
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __send_output_msgs(self, title, score, text):
        book_title, authors, categories, decade = self.book_data[title].split(",")
        compact_review = f"{book_title},{authors},{score},{decade}"
        full_review = f"{book_title},{categories},{text}"
        self.mq_connection_handler.send_message(self.output_queue_name_compact_reviews, compact_review)
        self.mq_connection_handler.send_message(self.output_queue_name_full_reviews, full_review)
        
    def __handle_eof_reviews(self):
        for line in self.reviews_buffer:
            title, score, text = line.split(",")
            if title in self.book_data:
                self.__send_output_msgs(title, score, text)
        self.reviews_buffer = []
        self.mq_connection_handler.send_message(self.output_queue_name_compact_reviews, "EOF")
        self.mq_connection_handler.send_message(self.output_queue_name_full_reviews, "EOF")
                    
        

            
            
                    
                    

        
    