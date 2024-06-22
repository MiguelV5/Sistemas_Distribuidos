from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
import csv
import io
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


TITLE_IDX = 0
AUTHORS_IDX = 1
PUBLISHER_IDX = 2
YEAR_IDX = 3


class FilterByTitle(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 title_keyword: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.output_queue = output_queue_name
        self.title_keyword = title_keyword
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__filter_books_by_title)
        
            
    def __filter_books_by_title(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            self.update_self_seq_number(body.client_id, seq_num_to_send)
        else:
            books = body.get_batch_iter_from_payload()
            payload_to_send = ""
            for book in books:
                title = book[TITLE_IDX]
                authors = book[AUTHORS_IDX]
                publisher = book[PUBLISHER_IDX]
                year = book[YEAR_IDX]
                if self.title_keyword.lower() in title.lower():
                    payload_to_send += f"{title},\"{authors}\",{publisher},{year}" + "\n"
            if payload_to_send:
                seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
                self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
                self.update_self_seq_number(body.client_id, seq_num_to_send)


    def start(self):
        self.mq_connection_handler.channel.start_consuming()