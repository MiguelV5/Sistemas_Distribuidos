from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

class Generator(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.results_msg = "[Q1 Results]:  (Title, Authors, Publisher, Publication Year)"
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue: [self.output_queue]}, 
                                                         input_exchange_name=input_exchange_name, 
                                                         input_queues_to_recv_from=[input_queue_name])
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__get_results)
        
    def start(self):
        self.mq_connection_handler.start_consuming()
        
    def __get_results(self, body: SystemMessage):
        msg = body.payload
        if body.type == SystemMessageType.EOF_B:
            logging.info("Received EOF")
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, 0, self.results_msg).encode_to_str())
            logging.info("Sending Q1 results to output queue")
        else: 
            self.results_msg += "\n" + msg 
        
        