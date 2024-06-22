import logging
import multiprocessing
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import QueryMessage, QueryMessageType, SystemMessage, SystemMessageType
from shared.socket_connection_handler import SocketConnectionHandler
import socket
from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import signal


AMOUNT_OF_QUERY_RESULTS = 5
class Server(MonitorableProcess):
    def __init__(self,server_port, 
                 input_exchange_of_query_results, 
                 input_exchange_of_mergers_confirms, 
                 input_queue_of_query_results, 
                 input_queue_of_mergers_confirms,
                 output_exchange_of_data, 
                 output_queue_of_reviews, 
                 output_queue_of_books,
                 mergers_quantity):
        self.controller_name_for_system_msgs = "server"
        self.state = {}
        self.state_file_path = f"{self.controller_name_for_system_msgs}_state.json"
        self.seq_num_for_system_msgs = 1

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('', server_port))
        self.server_socket.listen()
        self.client_sock = None
        self.server_is_running = True     
        self.finished_with_client_data = False
        self.mq_connection_handler = None
        self.required_merger_confirms = mergers_quantity

        self.input_exchange_of_query_results = input_exchange_of_query_results
        self.input_exchange_of_mergers_confirms = input_exchange_of_mergers_confirms
        self.input_queue_of_query_results = input_queue_of_query_results
        self.input_queue_of_mergers_confirms = input_queue_of_mergers_confirms
        self.output_exchange_of_data = output_exchange_of_data
        self.output_queue_of_reviews = output_queue_of_reviews
        self.output_queue_of_books = output_queue_of_books
        signal.signal(signal.SIGTERM, self.__handle_shutdown)


    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down server")
        self.server_is_running = False
        self.server_socket.close()
        if self.client_sock:
            self.client_sock.close()
        if self.mq_connection_handler:
            self.mq_connection_handler.close_connection()
    
    
    def run(self):
        incoming_sys_msgs_handler_process = multiprocessing.Process(target=self.__handle_incoming_sys_queues)
        incoming_sys_msgs_handler_process.start()
        
        self.__listen_to_clients()
        incoming_sys_msgs_handler_process.join()


    def __listen_to_clients(self):
        joinable_processes = []
        while self.server_is_running:
            self.client_sock, _ = self.server_socket.accept()
            if self.client_sock is not None:
                p = multiprocessing.Process(target=self.__handle_client_incoming_connection)
                joinable_processes.append(p)
                p.start()
                self.client_sock = None

        for p in joinable_processes:
            p.join()
            joinable_processes.remove(p)


    # ==============================================================================================================


    def __handle_incoming_sys_queues(self):
        self.mq_connection_handler = MQConnectionHandler(None,
                                                         None,
                                                         self.input_exchange_of_query_results,
                                                         [self.input_queue_of_query_results, self.input_queue_of_mergers_confirms],
                                                         self.input_exchange_of_mergers_confirms)
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_query_results, self.state_handler_callback, self.__process_msgs_from_sinks)
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_mergers_confirms, self.state_handler_callback, self.__process_mergers_confirms)
        self.mq_connection_handler.start_consuming()
        

    def __process_msgs_from_sinks(self, body: SystemMessage):
        if body.type == SystemMessageType.DATA:
            msg_for_client = QueryMessage(QueryMessageType.SV_RESULT, body.client_id, body.payload).encode_to_str()
            self.__send_direct_msg_to_client(body.client_id, msg_for_client)
        elif body.type == SystemMessageType.EOF_B or body.type == SystemMessageType.EOF_R:
            logging.info(f"Received EOF from [ {body.controller_name} ] for [ client_{body.client_id} ]")
            results_received_from_sinks = self.state[body.client_id].get("results_sent_to_client", 0) + 1
            self.state[body.client_id].update({"results_sent_to_client": results_received_from_sinks})
            logging.info(f"[ {results_received_from_sinks} ] results fully sent to [ client_{body.client_id} ]")
            if results_received_from_sinks == AMOUNT_OF_QUERY_RESULTS:
                logging.info(f"Sent all results for [ client_{body.client_id} ]. Sending SV_FINISHED message.\n")
                msg_for_client = QueryMessage(QueryMessageType.SV_FINISHED, body.client_id).encode_to_str()
                self.__send_direct_msg_to_client(body.client_id, msg_for_client)


    def __process_mergers_confirms(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            logging.info(f"Received EOF_B confirmation from [ {body.controller_name} ] for [ client_{body.client_id} ]")
            received_confirms = self.state[body.client_id].get("received_mergers_confirms", 1)
            logging
            should_send_continue_msg = (received_confirms == self.required_merger_confirms)
            if should_send_continue_msg:
                logging.info(f"Sending CONTINUE message to [ client_{body.client_id} ]")
                msg_for_client = QueryMessage(QueryMessageType.CONTINUE, body.client_id).encode_to_str()
                self.__send_direct_msg_to_client(body.client_id, msg_for_client)
            self.state[body.client_id].update({"received_mergers_confirms": received_confirms + 1})


    def __send_direct_msg_to_client(self, client_id, msg_for_client):
        client_name = f"client_{client_id}"
        client_responses_sock = SocketConnectionHandler.connect_and_create(client_name, constants.CLIENT_RESULTS_PORT)
        client_responses_sock.send_message(msg_for_client)

    
    # ==============================================================================================================

   

    def __handle_client_incoming_connection(self):
        output_queues_handler = MQConnectionHandler(self.output_exchange_of_data,
                                                    {self.output_queue_of_reviews: [self.output_queue_of_reviews], 
                                                     self.output_queue_of_books: [self.output_queue_of_books]},
                                                    None,
                                                    None)
        try:
            connection_handler = SocketConnectionHandler.create_from_socket(self.client_sock)  
            self.__handle_client_msgs(connection_handler, output_queues_handler)
        except Exception as e:
            logging.error("Error handling client connection: {}".format(str(e)))
        finally:
            connection_handler.close()
            output_queues_handler.close_connection()
            self.client_sock.close()
            
            
    def __handle_client_msgs(self, 
                             client_connection_handler: SocketConnectionHandler, 
                             output_queues_handler: MQConnectionHandler):
        try:           
            self.finished_with_client_data = False
            while not self.finished_with_client_data:
                client_msg = QueryMessage.decode_from_str(client_connection_handler.read_message())
                response_for_client = QueryMessage(QueryMessageType.DATA_ACK, client_msg.client_id).encode_to_str()
                if client_msg.type == QueryMessageType.EOF_B:
                    response_for_client = QueryMessage(QueryMessageType.WAIT_FOR_SV, client_msg.client_id).encode_to_str()
                    sys_msg = SystemMessage(SystemMessageType.EOF_B, client_msg.client_id, self.controller_name_for_system_msgs, self.seq_num_for_system_msgs).encode_to_str()
                    output_queues_handler.send_message(self.output_queue_of_books, sys_msg)
                    client_connection_handler.send_message(response_for_client)
                elif client_msg.type == QueryMessageType.EOF_R:
                    sys_msg = SystemMessage(SystemMessageType.EOF_R, client_msg.client_id, self.controller_name_for_system_msgs, self.seq_num_for_system_msgs).encode_to_str()
                    output_queues_handler.send_message(self.output_queue_of_reviews, sys_msg)
                    client_connection_handler.send_message(response_for_client)
                    self.finished_with_client_data = True
                elif client_msg.type == QueryMessageType.DATA_B:
                    sys_msg = SystemMessage(SystemMessageType.DATA, client_msg.client_id, self.controller_name_for_system_msgs, self.seq_num_for_system_msgs, client_msg.payload).encode_to_str()
                    output_queues_handler.send_message(self.output_queue_of_books, sys_msg)
                    client_connection_handler.send_message(response_for_client)
                elif client_msg.type == QueryMessageType.DATA_R:
                    sys_msg = SystemMessage(SystemMessageType.DATA, client_msg.client_id, self.controller_name_for_system_msgs, self.seq_num_for_system_msgs, client_msg.payload).encode_to_str()
                    output_queues_handler.send_message(self.output_queue_of_reviews, sys_msg)
                    client_connection_handler.send_message(response_for_client)

                self.seq_num_for_system_msgs += 1
        except OSError as e:
            logging.info(f"Client disconnected: {client_connection_handler.host}")           
        except Exception as e:
            raise e



