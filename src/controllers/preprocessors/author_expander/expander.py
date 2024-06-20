from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
import io
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

AUTHORS_IDX = 0
DECADE_IDX = 1

class AuthorExpander(MonitorableProcess):
    def __init__(self, 
                 input_exchange, 
                 output_exchange, 
                 input_queue_of_books, 
                 output_queues: dict[str,str],
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue_of_books = input_queue_of_books
        self.output_queues = {}
        for queue_name in output_queues.values():
            self.output_queues[queue_name] = [queue_name]
        self.mq_connection_handler = None
 

        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange,
                                                         output_queues_to_bind=self.output_queues,
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue_of_books])       
 
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_books, self.state_handler_callback, self.__expand_authors)
        self.mq_connection_handler.start_consuming()
        
    
    def __expand_authors(self, body: SystemMessage):
        """ 
        The body is a csv batch with the following format in a line: "['author_1',...,'author_n'], decade" 
        The expansion should create multiple lines, one for each author, with the following format: "author_i, decade"
        """
        logging.debug(f"Received message from input queue: {body.payload}")
        if body.type == SystemMessageType.EOF_B:
            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            for queue_name in self.output_queues:
                self.mq_connection_handler.send_message(queue_name, SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str())
            logging.info("Sent EOF message to output queues")
            self.update_self_seq_number(body.client_id, seq_num_to_send)
        else:
            books_batch = body.get_batch_iter_from_payload()
            payload_per_controller = {queue_name: "" for queue_name in self.output_queues.keys()}
            for book in books_batch:
                authors = eval(book[AUTHORS_IDX])
                decade = book[DECADE_IDX]
                for author in authors:
                    selected_queue_for_author = self.__select_queue(author)
                    payload_of_selected_queue = payload_per_controller.get(selected_queue_for_author, "")
                    updated_payload = payload_of_selected_queue + f"{author},{decade}" + "\n"
                    payload_per_controller[selected_queue_for_author] = updated_payload

            seq_num_to_send = self.get_seq_num_to_send(body.client_id, self.controller_name)
            for output_queue, payload in payload_per_controller.items():
                self.mq_connection_handler.send_message(output_queue, SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload).encode_to_str())
            self.update_self_seq_number(body.client_id, seq_num_to_send)




    def __select_queue(self, author: str) -> str:
        """
        Should return the queue name where the author should be sent to.
        It uses the hash of the author to select a queue on self.output_queues
        """
        
        hash_value = hash(author)
        queue_index = hash_value % len(self.output_queues)
        return list(self.output_queues.keys())[queue_index]    
            
        
        