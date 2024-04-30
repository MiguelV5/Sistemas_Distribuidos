import logging
from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import csv
import io
import signal

TITLE_IDX = 0
AUTHORS_IDX = 1
SCORE_IDX = 2
DECADE_IDX = 3


class CounterOfReviewsPerBook:
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str
                 ):
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.books_reviews = {}
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name,
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]},
                                                         input_exchange_name=self.input_exchange_name,
                                                         input_queues_to_recv_from=[self.input_queue_name])
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down CounterOfReviewsPerBook")
        self.mq_connection_handler.stop_consuming()
        
    def start(self):
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name, self.__count_reviews)
        self.mq_connection_handler.channel.start_consuming()
    
    def __count_reviews(self, ch, method, properties, body):
        """
        The message should have the following format: title,authors,score,decade
        """
        msg = body.decode()
        logging.info(f"Received message from input queue: {msg}")
        if msg == constants.FINISH_MSG:
            self.__send_results()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.mq_connection_handler.close_connection()
            return
        else:
            review = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            for row in review:
                title = row[TITLE_IDX]
                self.books_reviews.setdefault(title, [row[TITLE_IDX],row[AUTHORS_IDX],list(),row[DECADE_IDX]])
                self.books_reviews[title][SCORE_IDX].append(row[SCORE_IDX])
                ch.basic_ack(delivery_tag=method.delivery_tag)                    
                    
    
    def __send_results(self):
        for title,review in self.books_reviews.items():
            for i in range(len(review[SCORE_IDX])):
                output_msg = ""
                output_msg += f"{title},\"{review[AUTHORS_IDX]}\",{review[SCORE_IDX][i]},{review[DECADE_IDX]},{len(review[SCORE_IDX])}" + '\n'
                self.mq_connection_handler.send_message(self.output_queue_name, output_msg)
                logging.info(f"Sent message to output queue: {output_msg}")
        self.mq_connection_handler.send_message(self.output_queue_name, constants.FINISH_MSG)
        logging.info("Sent EOF message to output queue")
        self.mq_connection_handler.close_connection