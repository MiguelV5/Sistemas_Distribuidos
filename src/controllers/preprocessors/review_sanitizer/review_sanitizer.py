import io
from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
from shared import constants
from shared.monitorable_process import MonitorableProcess


TITLE_IDX = 1
REVIEW_SCORE_IDX = 6
REVIEW_SUMMARY_IDX = 8
REVIEW_TEXT_IDX = 9
REQUIRED_SIZE_OF_ROW = 10


class ReviewSanitizer(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 input_queue: str, 
                 output_exchange: str, 
                 output_queues: list[str],
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queues = output_queues
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue: [output_queue] for output_queue in output_queues},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__sanitize_batch_of_reviews)


    def __sanitize_batch_of_reviews(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            for output_queue in self.output_queues:
                self.mq_connection_handler.send_message(output_queue, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            batch_as_csv = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batches_to_send_towards_mergers = {output_queue: "" for output_queue in self.output_queues}
            for row in batch_as_csv:
                if len(row) != REQUIRED_SIZE_OF_ROW:
                    continue
                title = row[TITLE_IDX]
                review_score = row[REVIEW_SCORE_IDX]
                review_text = row[REVIEW_TEXT_IDX]
                
                if not title or not review_score or not review_text:
                    continue

                title = self.__fix_title_format(title)
                review_text = self.__fix_review_text_format(review_text)

                selected_queue = self.__select_queue(title)
                batches_to_send_towards_mergers[selected_queue] += f"{title},{round(float(review_score))},{review_text}" + "\n"

            for output_queue in self.output_queues:
                if batches_to_send_towards_mergers[output_queue]:
                    self.mq_connection_handler.send_message(output_queue, batches_to_send_towards_mergers[output_queue])
            
            ch.basic_ack(delivery_tag=method.delivery_tag)


    def __fix_title_format(self, title):
        return title.replace("\n", " ").replace("\r", "").replace(",", ";").replace('"', "`").replace("'", "`")
    
    def __fix_review_text_format(self, review_text):
        return review_text.replace("\n", " ").replace("\r", "").replace(",", ";").replace('"', "'").replace("&quot;", "'")

    def __select_queue(self, title: str) -> str:
        """
        Should return the queue name where the review should be sent to.
        It uses the hash of the title to select a queue on self.output_queues
        """
        hash_value = hash(title)
        queue_index = hash_value % len(self.output_queues)
        return self.output_queues[queue_index]


    def start(self):
        self.mq_connection_handler.start_consuming()
