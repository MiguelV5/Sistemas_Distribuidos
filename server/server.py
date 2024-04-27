import logging
import multiprocessing
from shared.socket_connection_handler import SocketConnectionHandler
import socket
from shared.mq_connection_handler import MQConnectionHandler
from shared import constants
import signal


class Server:
    def __init__(self,server_port, input_exchange, input_queue_of_query_results, output_exchange_of_data, output_queue_of_reviews, output_queue_of_books):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('', server_port))
        self.server_socket.listen()
        self.client_sock = None
        self.server_is_running = True        

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
    
    
    def run(self):
        process = multiprocessing.Process(target=self.__handle_results_queue, args=())
        process.start()
        
        self.__listen_to_client()
        for process in multiprocessing.active_children():
            process.join()
    
            
    def __listen_to_client(self):
        while self.server_is_running:
            self.client_sock, _ = self.server_socket.accept()
            if self.client_sock is not None:
                self.__handle_client_connection()
         

    def __handle_results_queue(self):
        try:
            mq_connection_handler = MQConnectionHandler(None,
                                                        None,
                                                        self.input_exchange,
                                                        [self.input_queue_of_query_results]
                                                        )
            
            mq_connection_handler.setup_callback_for_input_queue(self.input_queue_of_query_results, self.__process_query_result)
            mq_connection_handler.start_consuming()
        except Exception as e:
            logging.error("Error handling results queue: {}".format(str(e)))
        
    def __process_query_result(self, ch, method, properties, body):
        # TODO
        pass


    def __handle_client_connection(self):
        try:
            output_queues_handler = MQConnectionHandler(self.output_exchange_of_data,
                                                        {self.output_queue_of_reviews: [self.output_queue_of_reviews], 
                                                         self.output_queue_of_books: [self.output_queue_of_books]},
                                                        None,
                                                        None
                                                        )
            connection_handler = SocketConnectionHandler(self.client_sock)
            message = connection_handler.read_message()
            if message == constants.START_BOOKS_MSG:
                logging.info("Starting books data receiving")
                self.__handle_incoming_client_data(connection_handler, output_queues_handler, self.output_queue_of_books)
            elif message == constants.START_REVIEWS_MSG:
                logging.info("Starting reviews data receiving")
                self.__handle_incoming_client_data(connection_handler, output_queues_handler, self.output_queue_of_reviews)
        except Exception as e:
            logging.error("Error handling client connection: {}".format(str(e)))
        finally:
            self.client_sock.close()
            
    def __handle_incoming_client_data(self, connection_handler: SocketConnectionHandler, output_queues_handler: MQConnectionHandler, queue_name: str):
        try:           
            while True:
                message = connection_handler.read_message()
                if message == constants.FINISH_MSG:
                    logging.info("Finished receiving file data")
                    output_queues_handler.send_message(queue_name, message)
                    break
                connection_handler.send_message(constants.OK_MSG)
                output_queues_handler.send_message(queue_name, message)
        except Exception as e:
            logging.error("Error handling file data: {}".format(str(e)))
            connection_handler.send_message("Error")
            
