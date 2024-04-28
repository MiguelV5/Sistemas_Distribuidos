from shared.mq_connection_handler import MQConnectionHandler
import signal
import logging
import csv
import io
from shared import constants

TITLE_IDX = 0
AUTHORS_IDX = 2
PUBLISHER_IDX = 5
PUBLISHED_DATE_IDX = 6
CATEGORIES_IDX = 8
ORIGINAL_SIZE_OF_ROW = 10

class BookSanitizer:

    def __init__(self, input_exchange: str, input_queue: str, output_exchange: str, output_queue: str):
        self.output_queue = output_queue
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue: [output_queue]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__sanitize_batch_of_books)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down book_sanitizer")
        self.mq_connection_handler.close_connection()


    def __sanitize_batch_of_books(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.mq_connection_handler.close_connection()
        else:
            batch_as_csv = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batch_to_send = ""
            for row in batch_as_csv:
                if len(row) < ORIGINAL_SIZE_OF_ROW:
                    continue
                title = row[TITLE_IDX]
                authors = row[AUTHORS_IDX]
                publisher = row[PUBLISHER_IDX]
                published_date = row[PUBLISHED_DATE_IDX]
                categories = row[CATEGORIES_IDX]
                if not title or not authors or not publisher or not published_date or not categories:
                    continue

                title = title.replace("\n", " ").replace("\r", "").replace(",", ";").replace('"', "'")

                batch_to_send += f"{title},{authors},{publisher},{published_date},{categories}" + "\n"

            if batch_to_send:
                self.mq_connection_handler.send_message(self.output_queue, batch_to_send)
            ch.basic_ack(delivery_tag=method.delivery_tag)

                


    def start(self):
        self.mq_connection_handler.start_consuming()
