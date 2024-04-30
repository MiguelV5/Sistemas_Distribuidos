import logging
from shared.socket_connection_handler import SocketConnectionHandler
from shared import constants
import socket
import signal

class Client:
    def __init__(self, server_ip, server_port, reviews_file_path, books_file_path, batch_size):
        self.server_ip = server_ip
        self.server_port = server_port
        self.reviews_file_path = reviews_file_path
        self.books_file_path = books_file_path
        self.batch_size = batch_size
        self.connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down client")
        if self.connection_handler:
            self.connection_handler.close()

    def start(self):
        logging.info("Starting client")
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((self.server_ip, self.server_port))           
            self.connection_handler = SocketConnectionHandler(server_socket)
            logging.info("Connected to server at {}:{}".format(self.server_ip, self.server_port))
            self.send_file_data(self.books_file_path, "books")
            self.send_file_data(self.reviews_file_path, "reviews")       
            self.receive_results() 
        except Exception as e:
            logging.error("Failed to connect to server: {}".format(str(e)))
            
    def send_file_data(self, file, type_of_content_in_file):
        """
        Sends the file data to the server
        """
        with open(file, 'r') as file:
            self.connection_handler.send_message(f"Start: {type_of_content_in_file}")
            completed = False

            _csv_header = file.readline()
            while not completed:
                batch = ""
                for _ in range(self.batch_size):                    
                    line = file.readline()
                    if not line or line == "\n":
                        completed = True
                        break
                    batch += line
                if not batch:
                    break
                self.connection_handler.send_message(batch)   
                response = self.connection_handler.read_message()
                if response != constants.OK_MSG:
                    logging.error("Server response: {}".format(response))
                    break  
        self.connection_handler.send_message(constants.FINISH_MSG)
        logging.info(f"Finish data sent to server. File: {file}")

        
    def receive_results(self):
        """
        Receives the results from the server
        """
        finished_receiving = False
        while finished_receiving is False:
            results = self.connection_handler.read_message()
            if results == constants.FINISH_MSG:
                finished_receiving = True
            else:
                logging.info(f"Results received from server: {results}")
        self.connection_handler.close()
        logging.info("Client connection closed")