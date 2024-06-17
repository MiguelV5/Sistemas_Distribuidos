from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


class FilterOfAuthorsByDecade(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 min_decades_to_filter: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.min_decades_to_filter = min_decades_to_filter
        self.mq_connection_handler = None
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]}, 
                                                         input_exchange_name=self.input_exchange_name, 
                                                         input_queues_to_recv_from=[self.input_queue_name])
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_name, self.state_handler_callback, self.__filter_authors_by_decades_quantity)
        self.mq_connection_handler.channel.start_consuming()
        
            
    def __filter_authors_by_decades_quantity(self, body: SystemMessage):
        msg = body.payload
        seq_num_to_send = self.get_seq_num_to_sendber(body.client_id, self.controller_name)
        logging.debug(f"Received message: {msg}")
        if body.type == SystemMessageType.EOF_B:
            logging.info("EOF received. Sending EOF message to output queue")
            self.mq_connection_handler.send_message(self.output_queue_name, SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
        else:
            author, decades = msg.split(",")
            if int(decades) >= int(self.min_decades_to_filter):
                self.mq_connection_handler.send_message(self.output_queue_name, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, msg).encode_to_str())
                logging.debug(f"Sent message to output queue: {msg}")
            else:
                logging.debug(f"Author {author} was filtered out. Decades: {decades} < {self.min_decades_to_filter}")
        self.update_self_seq_number(body.client_id, seq_num_to_send)
        
       