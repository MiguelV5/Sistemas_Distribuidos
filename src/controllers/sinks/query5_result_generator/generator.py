from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

TITLE_IDX = 0
AVG_POLARITY_IDX = 1

class Generator(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.response_payload = constants.PAYLOAD_HEADER_Q5
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__accumulate_results)
        
            
    def __accumulate_results(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            logging.info(f"Received EOF_R from [ client_{body.client_id} ]")
            next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.EOF_R, body.client_id, self.controller_name, next_seq_num).encode_to_str())
            self.update_self_seq_number(body.client_id, next_seq_num)
        else:
            self.response_payload += body.payload
            next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, next_seq_num, self.response_payload).encode_to_str())
            self.update_self_seq_number(body.client_id, next_seq_num)
            self.response_payload = constants.PAYLOAD_HEADER_Q3


    def start(self):
        self.mq_connection_handler.channel.start_consuming()

