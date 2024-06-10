from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import csv
import io
import logging
from shared.monitorable_process import MonitorableProcess

TITLE_IDX = 0
SCORES_IDX = 1

class Sorter(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 output_exchange: str, 
                 input_queue: str, 
                 output_queue: str, 
                 required_top_of_books: int,
                 worker_name: str):
        super().__init__(worker_name)
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.required_top_of_books = required_top_of_books
        self.best_books = []
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])
        
    def start(self):
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue, self.__sort_books)
        self.mq_connection_handler.start_consuming()
        
    def __sort_books(self, ch, method, properties, body):
        """
        The body is a csv line with the following format in the line: "title,[scores]"
        """
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue, f"\"{self.best_books}\"")
            self.mq_connection_handler.send_message(self.output_queue, constants.FINISH_MSG)
            self.best_books = []
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            book = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            for row in book:
                scores = eval(row[SCORES_IDX])
                scores = list(map(int, scores))
                avg_score = sum(scores) / len(scores)
                if len(self.best_books) < self.required_top_of_books:
                    self.best_books.append((row[TITLE_IDX], avg_score))
                    self.best_books.sort(key=lambda x: x[SCORES_IDX], reverse=True)
                else:
                    if avg_score > self.best_books[-1][SCORES_IDX]:
                        self.best_books.append((row[TITLE_IDX], avg_score))
                        self.best_books.sort(key=lambda x: x[SCORES_IDX], reverse=True)
                        self.best_books.pop()
            ch.basic_ack(delivery_tag=method.delivery_tag)
