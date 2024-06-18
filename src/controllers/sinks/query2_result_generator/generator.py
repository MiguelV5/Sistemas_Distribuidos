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
                 filters_quantity: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.response_msg = "[Q2 Results]:  (Author,NumberOfDecades)"
        self.eofs_received = {}
        self.filters_quantity = filters_quantity
        self.mq_connection_handler = None
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]}, 
                                                         input_exchange_name=self.input_exchange_name, 
                                                         input_queues_to_recv_from=[self.input_queue_name])
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_name, self.state_handler_callback, self.__get_results)
        self.mq_connection_handler.start_consuming()
        
    def __get_results(self, body: SystemMessage):
        msg = body.payload
        if body.type == SystemMessageType.EOF_B:
            client_eofs_received = self.eofs_received.get(body.client_id, 0) + 1
            self.eofs_received[body.client_id] = client_eofs_received
            if int(client_eofs_received) == int(self.filters_quantity):
                seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
                logging.info("Sending Q2 results to output queue: " + self.response_msg)
                self.mq_connection_handler.send_message(self.output_queue_name, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, self.response_msg).encode_to_str())  
        else:
            self.response_msg += "\n" + msg 

        
        