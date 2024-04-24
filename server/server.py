import logging
import multiprocessing
from shared.socket_connection_handler import SocketConnectionHandler
import socket
from shared.mq_connection_handler import MQConnectionHandler

class Server:
    def __init__(self, listen_backlog, server_port, input_exchange, output_exchange_of_reviews, output_exchange_of_books, output_queue_of_reviews, output_queue_of_books, input_queue_of_query_results):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', server_port))
        self._server_socket.listen(listen_backlog)
        self.server_is_running = True
        
        #TODO: Implement the MQConnectionHandler
        self.input_exchange = input_exchange
        self.output_exchange_of_reviews = output_exchange_of_reviews
        self.output_exchange_of_books = output_exchange_of_books
        self.output_queue_of_reviews = output_queue_of_reviews
        self.output_queue_of_books = output_queue_of_books
        self.input_queue_of_query_results = input_queue_of_query_results
        self.queues_handler = None
    
    def run(self):
        while self.server_is_running:
            client_sock = self.__accept_new_connection()
            if client_sock is not None:
                process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
                process.start()
        for process in multiprocessing.active_children():
            process.join()  
            
    def __handle_client_connection(self, client_sock):
        try:
            connection_handler = SocketConnectionHandler(client_sock)
            message = connection_handler.read_message()
            if message == "Start: books":
                logging.info("Starting books data receiving")
                self.__handle_file_data(connection_handler, self.output_queue_of_books )
            elif message == "Start: reviews":
                logging.info("Starting reviews data receiving")
                self.__handle_file_data(connection_handler, self.output_queue_of_reviews)
        except Exception as e:
            logging.error("Error handling client connection: {}".format(str(e)))
        finally:
            client_sock.close()
            
    def __handle_file_data(self, connection_handler: SocketConnectionHandler, queue_name: str):
        try:           
            while True:
                message = connection_handler.read_message()
                if message == "EOF":
                    break
                connection_handler.send_message("OK")
                self.__process_data(message, queue_name)
        except Exception as e:
            logging.error("Error handling file data: {}".format(str(e)))
            connection_handler.send_message("Error")
            
    def __process_data(self, data, queue_name):
        try:
            # TODO: Implement sending the message to the queue using the MQConnectionHandler
            pass
        except Exception as e:
            raise Exception("Error processing data: {}".format(str(e)))