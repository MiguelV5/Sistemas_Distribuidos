import logging
import multiprocessing
from shared.protocol_messages import SystemMessage, SystemMessageType
from shared.socket_connection_handler import SocketConnectionHandler
import socket
from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import signal

AMOUNT_OF_QUERY_RESULTS = 5
class Server:
    def __init__(self,server_port, input_exchange, input_queue_of_query_results, output_exchange_of_data, output_queue_of_reviews, output_queue_of_books):
        self.controller_name_for_system_msgs = "server"
        self.seq_num_for_system_msgs = 1

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('', server_port))
        self.server_socket.listen()
        self.client_sock = None
        self.server_is_running = True     
        self.finished_with_client_data = False
        self.received_query_results = 0
        self.mq_connection_handler = None

        self.input_exchange = input_exchange
        self.input_queue_of_query_results = input_queue_of_query_results
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
        pipe_receiver, pipe_sender = multiprocessing.Pipe()
        results_handler_process = multiprocessing.Process(target=self.__handle_results_from_queue, args=(pipe_sender,))
        results_handler_process.start()
        
        self.__listen_to_client(pipe_receiver)
        results_handler_process.join()


    # ==============================================================================================================


    def __handle_results_from_queue(self, pipe_sender):
        self.mq_connection_handler = MQConnectionHandler(None,
                                                         None,
                                                         self.input_exchange,
                                                         [self.input_queue_of_query_results])
        
        self.pipe_sender = pipe_sender
        self.mq_connection_handler.setup_callbacks_for_input_queue(self.input_queue_of_query_results, self.__process_query_result)
        self.mq_connection_handler.start_consuming()
        
    def __process_query_result(self, ch, method, properties, body):
        result = body.decode()            
        self.pipe_sender.send(result)
        self.received_query_results += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # ==============================================================================================================


    def __send_queries_results_to_client(self, 
                                         connection_handler: SocketConnectionHandler, 
                                         pipe_receiver):
        while self.received_query_results < AMOUNT_OF_QUERY_RESULTS:
            if pipe_receiver.poll(None):
                query_result = pipe_receiver.recv()
                connection_handler.send_message(query_result)
                self.received_query_results += 1
        connection_handler.send_message(constants.FINISH_MSG)
        self.received_query_results = 0
               
   
    def __listen_to_client(self, pipe_receiver):
        while self.server_is_running:
            self.client_sock, _ = self.server_socket.accept()
            if self.client_sock is not None:
                self.__handle_client_connection(pipe_receiver)
         


    def __handle_client_connection(self, pipe_receiver):
        output_queues_handler = MQConnectionHandler(self.output_exchange_of_data,
                                                    {self.output_queue_of_reviews: [self.output_queue_of_reviews], 
                                                     self.output_queue_of_books: [self.output_queue_of_books]},
                                                    None,
                                                    None)
        try:
            connection_handler = SocketConnectionHandler.create_from_socket(self.client_sock)
            self.finished_with_client_data = False
            while not self.finished_with_client_data:  
                message = connection_handler.read_message()
                if message == constants.START_BOOKS_MSG:
                    logging.info("Starting books data receiving")
                    self.__handle_incoming_client_data(connection_handler, output_queues_handler, self.output_queue_of_books)
                elif message == constants.START_REVIEWS_MSG:
                    logging.info("Starting reviews data receiving")
                    self.__handle_incoming_client_data(connection_handler, output_queues_handler, self.output_queue_of_reviews)
                    self.finished_with_client_data = True
            self.__send_queries_results_to_client(connection_handler, pipe_receiver)
        except Exception as e:
            logging.error("Error handling client connection: {}".format(str(e)))
            
            
    def __handle_incoming_client_data(self, client_connection_handler: SocketConnectionHandler, output_queues_handler: MQConnectionHandler, queue_name: str):
        try:           
            while True:
                message = client_connection_handler.read_message()
                sys_msg_type = SystemMessageType.DATA
                if message == constants.FINISH_MSG:
                    if queue_name == self.output_queue_of_books:
                        sys_msg_type = SystemMessageType.EOF_B
                        self.seq_num_for_system_msgs = 1
                    elif queue_name == self.output_queue_of_reviews:
                        sys_msg_type = SystemMessageType.EOF_R

                    sys_msg = SystemMessage(msg_type=sys_msg_type, 
                                            client_id=0, 
                                            controller_name=self.controller_name_for_system_msgs, 
                                            controller_seq_num=self.seq_num_for_system_msgs).encode_to_str()
                    output_queues_handler.send_message(queue_name, sys_msg)
                    self.seq_num_for_system_msgs += 1
                    break
                else:
                    sys_msg = SystemMessage(msg_type=sys_msg_type, 
                                            client_id=0, 
                                            controller_name=self.controller_name_for_system_msgs, 
                                            controller_seq_num=self.seq_num_for_system_msgs, 
                                            payload=message).encode_to_str()
                    output_queues_handler.send_message(queue_name, sys_msg)
                    self.seq_num_for_system_msgs += 1
                    client_connection_handler.send_message(constants.OK_MSG)
        except Exception as e:
            logging.error("Error handling file data: {}".format(str(e)))
            self.finished_with_client_data = True
            client_connection_handler.send_message("Error")
            
