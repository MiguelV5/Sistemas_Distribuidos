import io
from shared.mq_connection_handler import MQConnectionHandler
import signal
import logging
import csv
from shared import constants


TITLE_IDX = 0
AUTHORS_IDX = 1
YEAR_IDX = 2
CATEGORIES_IDX = 3
ORIGINAL_SIZE_OF_ROW = 4

class DecadePreprocessor:
    def __init__(self, input_exchange: str, input_queue: str, output_exchange: str, output_queue_towards_expander: str, output_queues_towards_mergers: list[str]):
        self.output_queue_towards_expander = output_queue_towards_expander
        self.output_queues_towards_mergers = output_queues_towards_mergers
        
        output_queues_to_bind = {output_queue_towards_expander: [output_queue_towards_expander]}
        for output_queue_towards_merger in output_queues_towards_mergers:
            output_queues_to_bind[output_queue_towards_merger] = [output_queue_towards_merger]

        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         output_queues_to_bind,
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__preprocess_batch)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down book_sanitizer")
        self.mq_connection_handler.close_connection()


    def __preprocess_batch(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            for output_queue in self.output_queues_towards_mergers:
                self.mq_connection_handler.send_message(output_queue, msg)
            self.mq_connection_handler.send_message(self.output_queue_towards_expander, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.mq_connection_handler.close_connection()
        else:
            batch = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batch_to_send_towards_expander = ""
            batches_to_send_towards_mergers = {output_queue: "" for output_queue in self.output_queues_towards_mergers}
            for row in batch:
                if len(row) < ORIGINAL_SIZE_OF_ROW:
                    continue
                title = row[TITLE_IDX]
                authors = row[AUTHORS_IDX]
                year = row[YEAR_IDX]
                categories = row[CATEGORIES_IDX]
                decade = self.__extract_decade(year)

                batch_to_send_towards_expander += f"\"{authors}\",{decade}" + "\n"
                selected_merger_queue = self.__select_merger_queue(title)
                batches_to_send_towards_mergers[selected_merger_queue] += f"{title},\"{authors}\",\"{categories}\",{decade}" + "\n"
            
            if batch_to_send_towards_expander:
                self.mq_connection_handler.send_message(self.output_queue_towards_expander, batch_to_send_towards_expander)
            for output_queue in self.output_queues_towards_mergers:
                if batches_to_send_towards_mergers[output_queue]:
                    self.mq_connection_handler.send_message(output_queue, batches_to_send_towards_mergers[output_queue])

            ch.basic_ack(delivery_tag=method.delivery_tag)

    def __extract_decade(self, year: str) -> int:
        year = int(year)
        decade = year - (year % 10)
        return decade


    def __select_merger_queue(self, title: str) -> str:
        """
        Should return the queue name where the review should be sent to.
        It uses the hash of the title to select a queue on self.output_queue_towards_mergers
        """
        hash_value = hash(title)
        queue_index = hash_value % len(self.output_queues_towards_mergers)
        return self.output_queues_towards_mergers[queue_index]

    def start(self):
        self.mq_connection_handler.start_consuming()
