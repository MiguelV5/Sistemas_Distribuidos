import io
from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


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
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue, self.state_handler_callback, self.__process_msg_from_sv)


    def __process_msg_from_sv(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            self.__handle_eof(body)
        elif body.type == SystemMessageType.DATA:
            self.__sanitize_reviews_and_send(body)


    def __handle_eof(self, body: SystemMessage):
        logging.info(f"Received EOF_R from client: {body.client_id}")
        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        msg_to_send = SystemMessage(SystemMessageType.EOF_R, body.client_id, self.controller_name, seq_num_to_send).encode_to_str()
        for output_queue in self.output_queues:
            self.mq_connection_handler.send_message(output_queue, msg_to_send)
        self.update_self_seq_number(body.client_id, seq_num_to_send)

    def __sanitize_reviews_and_send(self, body: SystemMessage):
        reviews_batch = body.get_batch_iter_from_payload()
        payloads_to_send_towards_mergers = {output_queue: "" for output_queue in self.output_queues}
        for review in reviews_batch:
            if len(review) != REQUIRED_SIZE_OF_ROW:
                continue
            title = review[TITLE_IDX]
            review_score = review[REVIEW_SCORE_IDX]
            review_text = review[REVIEW_TEXT_IDX]
            if not title or not review_score or not review_text:
                continue

            title = self.__fix_title_format(title)
            review_text = self.__fix_review_text_format(review_text)

            selected_queue = self.__select_queue(title)
            payloads_to_send_towards_mergers[selected_queue] += self.__format_sanitized_review(title, review_score, review_text)

        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        for output_queue in self.output_queues:
            if payloads_to_send_towards_mergers[output_queue]:
                msg_to_send = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payloads_to_send_towards_mergers[output_queue]).encode_to_str()
                self.mq_connection_handler.send_message(output_queue, msg_to_send)
                self.update_self_seq_number(body.client_id, seq_num_to_send)    


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

    def __format_sanitized_review(self, title: str, review_score: str, review_text: str) -> str:
        return f"{title},{round(float(review_score))},{review_text}" + "\n"


    def start(self):
        self.mq_connection_handler.start_consuming()
