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
        self.response_payload = constants.PAYLOAD_HEADER_Q2
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
        if body.type == SystemMessageType.EOF_B:
            client_eofs_received = self.state.get(body.client_id, {}).get("eofs_received", 0) + 1
            self.state[body.client_id].update({"eofs_received": client_eofs_received})
            if int(client_eofs_received) == int(self.filters_quantity):
                logging.info(f"Received all EOFs from [ client_{body.client_id} ].")
                next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
                self.mq_connection_handler.send_message(self.output_queue_name, SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, next_seq_num).encode_to_str())
                self.update_self_seq_number(body.client_id, next_seq_num)
                self.state[body.client_id].update({"eofs_received": 0})
        else:
            self.response_payload += body.payload + "\n"
            next_seq_num = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue_name, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, next_seq_num, self.response_payload).encode_to_str())
            self.update_self_seq_number(body.client_id, next_seq_num)
            self.response_payload = constants.PAYLOAD_HEADER_Q2

        
        