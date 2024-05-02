from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import logging
import csv
import io
import signal

TITLE_IDX = 0
AUTHORS_IDX = 1
SCORE_IDX = 2
DECADE_IDX = 3
REVIEW_COUNT_IDX = 4

class FilterByReviewsCount:
    def __init__(self, input_exchange, output_exchange, input_queue, output_queue_towards_query3, output_queue_towards_sorter, min_reviews, num_of_counters):
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue_towards_query3 = output_queue_towards_query3
        self.output_queue_towards_sorter = output_queue_towards_sorter
        self.num_of_counters = int(num_of_counters)
        self.min_reviews = int(min_reviews)
        self.eofs_received = 0
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue_towards_query3: [self.output_queue_towards_query3], self.output_queue_towards_sorter: [output_queue_towards_sorter]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down CounterOfDecadesPerAuthor")
        self.mq_connection_handler.close_connection()        
        
    def start(self):
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue, self.__filter_books)
        self.mq_connection_handler.start_consuming()
        
    def __filter_books(self, ch, method, properties, body):
        """ 
        The body is a csv line with the following format in the line: "title,review_score,decade,reviews_count" 
        The filter should filter out books with reviews_count less than min_reviews and send the result to the outputsqueue.
        """
        msg = body.decode()
        logging.debug(f"Received message from input queue: {msg}")
        if msg == constants.FINISH_MSG:
            self.eofs_received += 1
            if self.eofs_received == self.num_of_counters:
                logging.info("Received EOF from all counters. Sending EOF to output queues.")
                self.mq_connection_handler.send_message(self.output_queue_towards_query3, constants.FINISH_MSG)
                self.mq_connection_handler.send_message(self.output_queue_towards_sorter, constants.FINISH_MSG)
                self.eofs_received = 0
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            review = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            for row in review:
                reviews_count = int(row[REVIEW_COUNT_IDX])
                if reviews_count >= self.min_reviews:
                    self.mq_connection_handler.send_message(self.output_queue_towards_query3, f"{row[TITLE_IDX]},{reviews_count},\"{row[AUTHORS_IDX]}\"")
                    logging.debug(f"Sent message to output queue towards query3:{self.output_queue_towards_query3} : {row[TITLE_IDX]},{reviews_count},\"{row[AUTHORS_IDX]}\"")
                    self.mq_connection_handler.send_message(self.output_queue_towards_sorter, f"{row[TITLE_IDX]},\"{row[SCORE_IDX]}\"")
                    logging.debug(f"Sent message to output queue towards sorter: {self.output_queue_towards_sorter}{row[TITLE_IDX]},\"{row[SCORE_IDX]}\"")
                else:
                    logging.debug(f"Discarded message: {row[TITLE_IDX]}. Reviews count: {row[REVIEW_COUNT_IDX]} < {self.min_reviews}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        