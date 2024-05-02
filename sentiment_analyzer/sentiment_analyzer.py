from shared.mq_connection_handler import MQConnectionHandler
import logging
import signal
from shared import constants
from textblob import TextBlob
import math

TITLE_IDX = 0
TEXT_IDX = 1

POLARITY_IDX = 0
TOTAL_REVIEWS_IDX = 1

class SentimentAnalyzer:
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str):
        self.output_queue = output_queue_name
        self.polarity_accumulator = PolarityAccumulator()
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue_name, self.__handle_reviews_calculations)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Sentiment Analyzer")
        self.mq_connection_handler.close_connection()
        
            
    def __handle_reviews_calculations(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            logging.info("Received EOF message")
            self.__handle_eof_reviews()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            batch_lines = msg.split("\n")
            for line in batch_lines:
                if line:
                    book_data = line.split(",")
                    title = book_data[TITLE_IDX]
                    text = book_data[TEXT_IDX]
                    logging.debug(f"Calculating polarity for book {title}")
                    polarity = TextBlob(text).sentiment.polarity
                    self.polarity_accumulator.add_polarity_for_book(title, polarity)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __handle_eof_reviews(self):
        while self.polarity_accumulator.get_total_books() > 0:
            title, polarity_mean = self.polarity_accumulator.pop_average_polarity_of_book()
            self.mq_connection_handler.send_message(self.output_queue, f"{title},{polarity_mean}")
        self.mq_connection_handler.send_message(self.output_queue, constants.FINISH_MSG)
        logging.info("Sent EOF message to output queue")
        self.polarity_accumulator = PolarityAccumulator()
        
        
    def start(self):
        self.mq_connection_handler.channel.start_consuming()



class PolarityAccumulator:
    def __init__(self):
        # Contains book titles as keys and a list with the sum of the polarity of the reviews and the total number of reviews for that book as values
        self.accumulator_per_book = {}
        self.total_books = 0

    def add_polarity_for_book(self, title: str, polarity: float):
        if title in self.accumulator_per_book:
            self.accumulator_per_book[title][POLARITY_IDX] = math.fsum(
                [self.accumulator_per_book[title][POLARITY_IDX], polarity]
            )
            self.accumulator_per_book[title][TOTAL_REVIEWS_IDX] += 1
        else:
            self.accumulator_per_book[title] = [polarity, 1]
            self.total_books += 1

    def pop_average_polarity_of_book(self) -> tuple[str, float]:
        """
        Returns the mean of the polarity of a single book and removes it from the accumulator
        """
        title, acc_values = self.accumulator_per_book.popitem()
        self.total_books -= 1
        polarity = acc_values[POLARITY_IDX]
        total_reviews = acc_values[TOTAL_REVIEWS_IDX]
        polarity_mean = polarity / total_reviews
        return title, polarity_mean
    
    def get_total_books(self) -> int:
        return self.total_books