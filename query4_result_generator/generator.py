from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess

TITLE_IDX = 0
SCORES_IDX = 1

class Generator(MonitorableProcess):
    def __init__(self, input_exchange, output_exchange, input_queue, output_queue):
        super().__init__()
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.output_msg = "[Q4 Results]:  (Title, ReviewScore)"
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue])
        
    def start(self):
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue, self.__generate)
        self.mq_connection_handler.start_consuming()
        
    def __generate(self, ch, method, properties, body):
        """
        The body is a csv list with the following: "[(title,review_score)]" 
        The generator should send the result to the output queue.
        """
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.output_msg = "[Q4 Results]:  (Title, ReviewScore)"
        else:
            books = eval(msg.replace("\"",""))
            for book in books:
                self.output_msg += "\n" f"{book[TITLE_IDX]},{book[SCORES_IDX]}"
            logging.info("Sending Q4 results to output queue")
            self.mq_connection_handler.send_message(self.output_queue, self.output_msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)

