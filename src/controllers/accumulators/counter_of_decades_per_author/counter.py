from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType


AUTHOR_IDX = 0
DECADE_IDX = 1

class CounterOfDecadesPerAuthor(MonitorableProcess):
    def __init__(self, 
                 input_exchange: str, 
                 output_exchange: str, 
                 input_queue_of_authors: str, 
                 output_queue_of_authors: str,
                 controller_name: str):
        super().__init__(controller_name)
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue_of_authors = input_queue_of_authors
        self.output_queue_of_authors = output_queue_of_authors
        self.mq_connection_handler = None
        self.__parse_decades_state()
        
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange,
                                                         output_queues_to_bind={self.output_queue_of_authors: [self.output_queue_of_authors]},
                                                         input_exchange_name=self.input_exchange,
                                                         input_queues_to_recv_from=[self.input_queue_of_authors])
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_authors, self.state_handler_callback,self.__count_authors)
        self.mq_connection_handler.start_consuming()
            
    def __count_authors(self, body: SystemMessage):
        """ 
        The body is a csv line with the following format in the line: "author, decade" 
        The counter should count the number of authors per decade and send the result to the output queue.
        """
        logging.debug(f"Received message: {body.payload}")
        if body.type == SystemMessageType.EOF_B:
            logging.info(f"Received EOF_B from [ client_{body.client_id} ]. Sending results and EOF to output queue")
            self.__send_results(body.client_id)
        else:
            authors_decades_batch = body.get_batch_iter_from_payload()
            for author_decade in authors_decades_batch:
                author = author_decade[AUTHOR_IDX]
                decade = author_decade[DECADE_IDX]
                self.__update_authors_decades_per_client(body.client_id, author, decade)
        
    def __send_results(self, client_id):
        for author, decades in self.state[client_id]["authors_decades"].items():
            seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
            payload_to_send = ""
            payload_to_send += author + ',' + str(len(decades))
            self.mq_connection_handler.send_message(self.output_queue_of_authors, SystemMessage(SystemMessageType.DATA, client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
            self.update_self_seq_number(client_id, seq_num_to_send)
            
        seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
        self.mq_connection_handler.send_message(self.output_queue_of_authors, SystemMessage(SystemMessageType.EOF_B, client_id, self.controller_name, seq_num_to_send).encode_to_str())   
        self.update_self_seq_number(client_id, seq_num_to_send)
        
    def __update_authors_decades_per_client(self, client_id, author, decade):
        if client_id in self.state:
            if "authors_decades" in self.state[client_id]:
                if author in self.state[client_id]["authors_decades"]:
                    self.state[client_id]["authors_decades"][author].add(decade)
                else:
                    self.state[client_id]["authors_decades"][author] = set([decade])
            else:
                self.state[client_id]["authors_decades"] = {author: set([decade])}
        else:
            self.state[client_id] = {"authors_decades": {author: set([decade])}}
            
    def __parse_decades_state(self):   
        # Convert list returned by json to set
        for client_id, client_data in self.state.items():
            if "authors_decades" in client_data:
                for author, decades in client_data["authors_decades"].items():
                    self.state[client_id]["authors_decades"][author] = set(decades)