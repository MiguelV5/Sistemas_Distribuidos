from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import logging
import csv
import io
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

TITLE_IDX = 0
AUTHORS_IDX = 2
REVIEW_COUNT_IDX = 1

class Generator(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 output_exchange: str, 
                 input_queue: str, 
                 output_queue: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue: self.output_queue},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])
        self.response_msg = "[Q3 Results]:  (Title, Reviews, Authors)"
        
    def start(self):
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue, self.__generate_query3_result)
        self.mq_connection_handler.start_consuming()
        
    def __generate_query3_result(self, body: SystemMessage):
        """
        The body is a csv line with the following format in the line: "title,reviews_count,authors"
        """
        msg = body.payload
        if body.type == SystemMessageType.EOF_R:
            logging.info("Sending Q3 results to output queue")
            next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, next_seq_num, self.response_msg).encode_to_str())   
            self.response_msg = "[Q3 Results]:  (Title, Reviews, Authors)"
        else:
            review = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            for row in review:
                self.response_msg += "\n" + f"{row[TITLE_IDX]}, {row[REVIEW_COUNT_IDX]}, \"{row[AUTHORS_IDX]}\""