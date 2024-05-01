from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import signal
import csv
import io
import logging

TITLE_IDX = 0
SCORES_IDX = 1

class Sorter:
    def __init__(self, input_exchange, output_exchange, input_queue, output_queue, top_of_books):
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.top_of_books = int(top_of_books)
        self.best_books = []
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Sorter")
        self.mq_connection_handler.close_connection()
        
    def start(self):
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue, self.__sort_books)
        self.mq_connection_handler.start_consuming()
        
    def __sort_books(self, ch, method, properties, body):
        """
        The body is a csv line with the following format in the line: "title,[scores]"
        """
        msg = body.decode()
        # calculate the average of the scores, them put ordered by the average on the best_books list, which will have at most top_of_books elements
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue, f"\"{self.best_books}\"")
            logging.info(f"Sent message to output queue: {self.output_queue} : \"{self.best_books}\"")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        book = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
        for row in book:
            scores = eval(row[SCORES_IDX])
            scores = list(map(int, scores))
            avg_score = sum(scores) / len(scores)
            if len(self.best_books) < self.top_of_books:
                # we need to maintain the list ordered by the average score for every book, the top average will be the first element, if a new book has a higher average score than the first element, we need to insert the new book in the correct position and move the other elements to the right, removing the last element if the list has more than top_of_books elements
                
                self.best_books.append((row[TITLE_IDX], avg_score))
                self.best_books.sort(key=lambda x: x[SCORES_IDX], reverse=True)
            else:
                if avg_score > self.best_books[-1][SCORES_IDX]:
                    self.best_books.append((row[TITLE_IDX], avg_score))
                    self.best_books.sort(key=lambda x: x[SCORES_IDX], reverse=True)
                    self.best_books.pop()
        ch.basic_ack(delivery_tag=method.delivery_tag)
                    